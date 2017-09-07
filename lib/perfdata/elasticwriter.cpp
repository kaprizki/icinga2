/******************************************************************************
 * Icinga 2                                                                   *
 * Copyright (C) 2012-2017 Icinga Development Team (https://www.icinga.com/)  *
 *                                                                            *
 * This program is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU General Public License                *
 * as published by the Free Software Foundation; either version 2             *
 * of the License, or (at your option) any later version.                     *
 *                                                                            *
 * This program is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the              *
 * GNU General Public License for more details.                               *
 *                                                                            *
 * You should have received a copy of the GNU General Public License          *
 * along with this program; if not, write to the Free Software Foundation     *
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.             *
 ******************************************************************************/

#include "perfdata/elasticwriter.hpp"
#include "perfdata/elasticwriter.tcpp"
#include "remote/url.hpp"
#include "remote/httprequest.hpp"
#include "remote/httpresponse.hpp"
#include "icinga/compatutility.hpp"
#include "icinga/service.hpp"
#include "icinga/checkcommand.hpp"
#include "base/tcpsocket.hpp"
#include "base/stream.hpp"
#include "base/json.hpp"
#include "base/utility.hpp"
#include "base/networkstream.hpp"
#include "base/perfdatavalue.hpp"
#include "base/exception.hpp"
#include "base/statsfunction.hpp"
#include <boost/algorithm/string.hpp>

using namespace icinga;

REGISTER_TYPE(ElasticWriter);

REGISTER_STATSFUNCTION(ElasticWriter, &ElasticWriter::StatsFunc);

ElasticWriter::ElasticWriter(void)
	: m_WorkQueue(10000000, 1)
{ }

void ElasticWriter::OnConfigLoaded(void)
{
	ObjectImpl<ElasticWriter>::OnConfigLoaded();

	m_WorkQueue.SetName("ElasticWriter, " + GetName());
}

void ElasticWriter::StatsFunc(const Dictionary::Ptr& status, const Array::Ptr& perfdata)
{
	Dictionary::Ptr nodes = new Dictionary();

	for (const ElasticWriter::Ptr& elasticwriter : ConfigType::GetObjectsByType<ElasticWriter>()) {
		size_t workQueueItems = elasticwriter->m_WorkQueue.GetLength();
		double workQueueItemRate = elasticwriter->m_WorkQueue.GetTaskCount(60) / 60.0;

		Dictionary::Ptr stats = new Dictionary();
		stats->Set("work_queue_items", workQueueItems);
		stats->Set("work_queue_item_rate", workQueueItemRate);

		nodes->Set(elasticwriter->GetName(), stats);

		perfdata->Add(new PerfdataValue("elasticwriter_" + elasticwriter->GetName() + "_work_queue_items", workQueueItems));
		perfdata->Add(new PerfdataValue("elasticwriter_" + elasticwriter->GetName() + "_work_queue_item_rate", workQueueItemRate));
	}

	status->Set("elasticwriter", nodes);
}

void ElasticWriter::Start(bool runtimeCreated)
{
	ObjectImpl<ElasticWriter>::Start(runtimeCreated);

 	Log(LogInformation, "ElasticWriter")
	    << "'" << GetName() << "' started.";

	m_WorkQueue.SetExceptionCallback(boost::bind(&ElasticWriter::ExceptionHandler, this, _1));

	/* Setup timer for periodically flushing m_DataBuffer */
	m_FlushTimer = new Timer();
	//m_FlushTimer->SetInterval(GetFlushInterval());
	m_FlushTimer->SetInterval(10);
	m_FlushTimer->OnTimerExpired.connect(boost::bind(&ElasticWriter::FlushTimeout, this));
	m_FlushTimer->Start();
	m_FlushTimer->Reschedule(0);

	/* Register for new metrics. */
	Checkable::OnStateChange.connect(boost::bind(&ElasticWriter::StateChangeHandler, this, _1, _2, _3));
	Service::OnNewCheckResult.connect(boost::bind(&ElasticWriter::CheckResultHandler, this, _1, _2));
	Checkable::OnNotificationSentToUser.connect(boost::bind(&ElasticWriter::NotificationToUserHandler, this, _1, _2, _3, _4, _5, _6, _7, _8));
}

void ElasticWriter::Stop(bool runtimeRemoved)
{
	Log(LogInformation, "ElasticWriter")
	    << "'" << GetName() << "' stopped.";

	m_WorkQueue.Join();

	ObjectImpl<ElasticWriter>::Stop(runtimeRemoved);
}

void ElasticWriter::StateChangeHandler(const Checkable::Ptr& checkable, const CheckResult::Ptr& cr, StateType type)
{
	m_WorkQueue.Enqueue(boost::bind(&ElasticWriter::StateChangeHandlerInternal, this, checkable, cr, type));
}

void ElasticWriter::StateChangeHandlerInternal(const Checkable::Ptr& checkable, const CheckResult::Ptr& cr, StateType type)
{
	AssertOnWorkQueue();

	CONTEXT("Elasticwriter processing state change '" + checkable->GetName() + "'");

	Host::Ptr host;
	Service::Ptr service;
	tie(host, service) = GetHostService(checkable);

	Dictionary::Ptr fields = new Dictionary();

	fields->Set("_state", service ? Service::StateToString(service->GetState()) : Host::StateToString(host->GetState()));
	fields->Set("_current_check_attempt", checkable->GetCheckAttempt());
	fields->Set("_max_check_attempts", checkable->GetMaxCheckAttempts());
	fields->Set("_hostname", host->GetName());

	if (service) {
		fields->Set("_service_name", service->GetShortName());
		fields->Set("_service_state", Service::StateToString(service->GetState()));
		fields->Set("_last_state", service->GetLastState());
		fields->Set("_last_hard_state", service->GetLastHardState());
	} else {
		fields->Set("_last_state", host->GetLastState());
		fields->Set("_last_hard_state", host->GetLastHardState());
	}

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		fields->Set("_check_command", commandObj->GetName());

	AddToQueue("statechange", fields);
}

void ElasticWriter::CheckResultHandler(const Checkable::Ptr& checkable, const CheckResult::Ptr& cr)
{
	m_WorkQueue.Enqueue(boost::bind(&ElasticWriter::InternalCheckResultHandler, this, checkable, cr));
}

void ElasticWriter::InternalCheckResultHandler(const Checkable::Ptr& checkable, const CheckResult::Ptr& cr)
{
	AssertOnWorkQueue();

	CONTEXT("Elasticwriter processing check result for '" + checkable->GetName() + "'");

	if (!IcingaApplication::GetInstance()->GetEnablePerfdata() || !checkable->GetEnablePerfdata())
		return;

	Host::Ptr host;
	Service::Ptr service;
	boost::tie(host, service) = GetHostService(checkable);

	Dictionary::Ptr tmpl_fields = new Dictionary();

	if (service) {
		tmpl_fields->Set("_service_name", service->GetShortName());
		tmpl_fields->Set("_service_state", Service::StateToString(service->GetState()));
		tmpl_fields->Set("_last_state", service->GetLastState());
		tmpl_fields->Set("_last_hard_state", service->GetLastHardState());
	} else {
		tmpl_fields->Set("_last_state", host->GetLastState());
		tmpl_fields->Set("_last_hard_state", host->GetLastHardState());
	}

	tmpl_fields->Set("_hostname", host->GetName());
	tmpl_fields->Set("_state", service ? Service::StateToString(service->GetState()) : Host::StateToString(host->GetState()));

	tmpl_fields->Set("_current_check_attempt", checkable->GetCheckAttempt());
	tmpl_fields->Set("_max_check_attempts", checkable->GetMaxCheckAttempts());

	tmpl_fields->Set("_reachable", checkable->IsReachable());

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		tmpl_fields->Set("_check_command", commandObj->GetName());

	double ts = Utility::GetTime();

	if (cr) {
		tmpl_fields->Set("_latency", cr->CalculateLatency());
		tmpl_fields->Set("_execution_time", cr->CalculateExecutionTime());
		tmpl_fields->Set("short_message", CompatUtility::GetCheckResultOutput(cr));
		tmpl_fields->Set("full_message", cr->GetOutput());
		tmpl_fields->Set("_check_source", cr->GetCheckSource());
		ts = cr->GetExecutionEnd();
		Array::Ptr perfdata = cr->GetPerformanceData();

		if (perfdata) {
			ObjectLock olock(perfdata);
			for (const Value& val : perfdata) {
				PerfdataValue::Ptr pdv;
				Dictionary::Ptr fields = tmpl_fields->ShallowClone();

				if (val.IsObjectType<PerfdataValue>())
					pdv = val;
				else {
					try {
						pdv = PerfdataValue::Parse(val);
					} catch (const std::exception&) {
						Log(LogWarning, "ElasticWriter")
						    << "Ignoring invalid perfdata value: '" << val << "' for object '"
						    << checkable->GetName() << "'.";
					}
				}

				String escaped_key = pdv->GetLabel();
				boost::replace_all(escaped_key, " ", "_");
				boost::replace_all(escaped_key, ".", "_");
				boost::replace_all(escaped_key, "\\", "_");
				boost::algorithm::replace_all(escaped_key, "::", ".");

				fields->Set("_" + escaped_key, pdv->GetValue());

				if (pdv->GetMin())
					fields->Set("_" + escaped_key + "_min", pdv->GetMin());
				if (pdv->GetMax())
					fields->Set("_" + escaped_key + "_max", pdv->GetMax());
				if (pdv->GetWarn())
					fields->Set("_" + escaped_key + "_warn", pdv->GetWarn());
				if (pdv->GetCrit())
					fields->Set("_" + escaped_key + "_crit", pdv->GetCrit());
				AddToQueue("checkresult", fields);
			}
		} else {
			AddToQueue("checkresult", tmpl_fields);
		}
	}
}

void ElasticWriter::NotificationToUserHandler(const Notification::Ptr& notification, const Checkable::Ptr& checkable,
    const User::Ptr& user, NotificationType notificationType, CheckResult::Ptr const& cr,
    const String& author, const String& commentText, const String& commandName)
{
	m_WorkQueue.Enqueue(boost::bind(&ElasticWriter::NotificationToUserHandlerInternal, this,
	    notification, checkable, user, notificationType, cr, author, commentText, commandName));
}

void ElasticWriter::NotificationToUserHandlerInternal(const Notification::Ptr& notification, const Checkable::Ptr& checkable,
    const User::Ptr& user, NotificationType notificationType, CheckResult::Ptr const& cr,
    const String& author, const String& commentText, const String& commandName)
{
	AssertOnWorkQueue();

	CONTEXT("Elasticwriter processing notification to all users '" + checkable->GetName() + "'");

	Log(LogDebug, "ElasticWriter")
	    << "Processing notification for '" << checkable->GetName() << "'";

	Host::Ptr host;
	Service::Ptr service;
	tie(host, service) = GetHostService(checkable);

	String notificationTypeString = Notification::NotificationTypeToString(notificationType);

	String authorComment = "";

	if (notificationType == NotificationCustom || notificationType == NotificationAcknowledgement) {
		authorComment = author + ";" + commentText;
	}

	Dictionary::Ptr fields = new Dictionary();
	String type;

	if (service) {
		type = "servicenotification";
		fields->Set("_service_name", service->GetShortName());
	} else {
		type = "hostnotification";
	}

	fields->Set("_state", service ? Service::StateToString(service->GetState()) : Host::StateToString(host->GetState()));

	fields->Set("_hostname", host->GetName());
	fields->Set("_command", commandName);
	fields->Set("_notification_type", notificationTypeString);
	fields->Set("_comment", authorComment);

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		fields->Set("_check_command", commandObj->GetName());

	ElasticWriter::AddToQueue(type, fields);
}

void ElasticWriter::AddToQueue(String type, const Dictionary::Ptr& fields)
{
	// Atomically buffer the data point
	boost::mutex::scoped_lock lock(m_DataBufferMutex);

	// Every payload needs a line describing the index above
	// We do it this way to avoid problems with a near full queue
	String dat = "{ \"index\" : { \"_type\" : \"" + type + "\" } }\n" + JsonEncode(fields);
	m_DataBuffer.push_back(dat);

	// Flush if we've buffered too much to prevent excessive memory use
	if (static_cast<int>(m_DataBuffer.size()) >= GetFlushThreshold()) {
		Log(LogDebug, "ElasticWriter")
		    << "Data buffer overflow writing " << m_DataBuffer.size() << " data points";
		Flush();
	}
}

void ElasticWriter::FlushTimeout(void)
{
	// Prevent new data points from being added to the array, there is a
	// race condition where they could disappear
	boost::mutex::scoped_lock lock(m_DataBufferMutex);

	// Flush if there are any data available
	if (m_DataBuffer.size() > 0) {
		Log(LogDebug, "ElasticWriter")
		    << "Timer expired writing " << m_DataBuffer.size() << " data points";
		Flush();
	}
}

void ElasticWriter::Flush(void)
{
	// Ensure you hold a lock against m_DataBuffer so that things
	// don't go missing after creating the body and clearing the buffer
	String body = boost::algorithm::join(m_DataBuffer, "\n");
	m_DataBuffer.clear();

	Url::Ptr url = new Url();
	//TODO HTTPS
	//TODO AUTH
	url->SetScheme("http");
	url->SetHost(GetHost());
	url->SetPort(GetPort());

	std::vector<String> path;
	path.push_back(GetIndex()+ "-" + Utility::FormatDateTime("%Y.%m.%d", Utility::GetTime()));
	path.push_back("_bulk");
	url->SetPath(path);

	Stream::Ptr stream = Connect();
	HttpRequest req(stream);
	req.AddHeader("Accept", "application/json");
	req.AddHeader("content-type", "application/json");
	req.RequestMethod = "POST";
	req.RequestUrl = url;

	try {
		req.WriteBody(body.CStr(), body.GetLength());
		req.Finish();
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
			<< "Cannot write to TCP socket on host '" << GetHost() << "' port '" << GetPort() << "'.";
		throw ex;
	}

	HttpResponse resp(stream, req);
	StreamReadContext context;

	try {
		resp.Parse(context, true);
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
			<< "Cannot read from TCP socket from host '" << GetHost() << "' port '" << GetPort() << "'.";
		throw ex;
	}

	size_t responseSize = resp.GetBodySize();
	boost::scoped_array<char> buffer(new char[responseSize + 1]);
	resp.ReadBody(buffer.get(), responseSize);
	buffer.get()[responseSize] = '\0';

}

Stream::Ptr ElasticWriter::Connect(void)
{
	TcpSocket::Ptr socket = new TcpSocket();
	Log(LogNotice, "ElasticWriter")
	    << "Connecting to Elastic on host '" << GetHost() << "' port '" << GetPort() << "'.";
	try {
		socket->Connect(GetHost(), GetPort());
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
		    << "Can't connect to Elastic on host '" << GetHost() << "' port '" << GetPort() << "'.";
		throw ex;
	}
	return new NetworkStream(socket);
}

void ElasticWriter::AssertOnWorkQueue(void)
{
	ASSERT(m_WorkQueue.IsWorkerThread());
}

void ElasticWriter::ExceptionHandler(boost::exception_ptr exp)
{
	Log(LogCritical, "ElasticWriter", "Exception during Elastic operation: Verify that your backend is operational!");

	Log(LogDebug, "ElasticWriter")
		<< "Exception during Elastic operation: " << DiagnosticInformation(exp);
}
