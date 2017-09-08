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

	m_EventPrefix = "icinga2.event.";

 	Log(LogInformation, "ElasticWriter")
	    << "'" << GetName() << "' started.";

	m_WorkQueue.SetExceptionCallback(boost::bind(&ElasticWriter::ExceptionHandler, this, _1));

	/* Setup timer for periodically flushing m_DataBuffer */
	m_FlushTimer = new Timer();
	m_FlushTimer->SetInterval(GetFlushInterval());
	m_FlushTimer->OnTimerExpired.connect(boost::bind(&ElasticWriter::FlushTimeout, this));
	m_FlushTimer->Start();
	m_FlushTimer->Reschedule(0);

	/* Register for new metrics. */
	Checkable::OnNewCheckResult.connect(boost::bind(&ElasticWriter::CheckResultHandler, this, _1, _2));
	Checkable::OnStateChange.connect(boost::bind(&ElasticWriter::StateChangeHandler, this, _1, _2, _3));
	Checkable::OnNotificationSentToAllUsers.connect(boost::bind(&ElasticWriter::NotificationSentToAllUsersHandler, this, _1, _2, _3, _4, _5, _6, _7));
}

void ElasticWriter::Stop(bool runtimeRemoved)
{
	Log(LogInformation, "ElasticWriter")
	    << "'" << GetName() << "' stopped.";

	m_WorkQueue.Join();

	ObjectImpl<ElasticWriter>::Stop(runtimeRemoved);
}

void ElasticWriter::AddCheckResult(const Dictionary::Ptr& fields, const Checkable::Ptr& checkable, const CheckResult::Ptr& cr)
{
	String prefix = "check_result.";

	fields->Set(prefix + "latency", cr->CalculateLatency());
	fields->Set(prefix + "execution_time", cr->CalculateExecutionTime());
	fields->Set(prefix + "output", cr->GetOutput());
	fields->Set(prefix + "check_source", cr->GetCheckSource());

	Array::Ptr perfdata = cr->GetPerformanceData();

	if (perfdata) {
		ObjectLock olock(perfdata);
		for (const Value& val : perfdata) {
			PerfdataValue::Ptr pdv;

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

			String perfdataPrefix = prefix + "perfdata.";

			fields->Set(perfdataPrefix + escaped_key + ".value", pdv->GetValue());

			if (pdv->GetMin())
				fields->Set(perfdataPrefix + escaped_key + ".min", pdv->GetMin());
			if (pdv->GetMax())
				fields->Set(perfdataPrefix + escaped_key + ".max", pdv->GetMax());
			if (pdv->GetWarn())
				fields->Set(perfdataPrefix + escaped_key + ".warn", pdv->GetWarn());
			if (pdv->GetCrit())
				fields->Set(perfdataPrefix + escaped_key + ".crit", pdv->GetCrit());
		}
	}
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

	Dictionary::Ptr fields = new Dictionary();

	if (service) {
		fields->Set("service", service->GetShortName());
		fields->Set("state", service->GetState());
		fields->Set("last_state", service->GetLastState());
		fields->Set("last_hard_state", service->GetLastHardState());
	} else {
		fields->Set("state", host->GetState());
		fields->Set("last_state", host->GetLastState());
		fields->Set("last_hard_state", host->GetLastHardState());
	}

	fields->Set("host", host->GetName());
	fields->Set("state_type", checkable->GetStateType());

	fields->Set("current_check_attempt", checkable->GetCheckAttempt());
	fields->Set("max_check_attempts", checkable->GetMaxCheckAttempts());

	fields->Set("reachable", checkable->IsReachable());

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		fields->Set("check_command", commandObj->GetName());

	double ts = Utility::GetTime();

	if (cr) {
		AddCheckResult(fields, checkable, cr);
		ts = cr->GetExecutionEnd();
	}

	Enqueue("checkresult", fields, ts);
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

	fields->Set("current_check_attempt", checkable->GetCheckAttempt());
	fields->Set("max_check_attempts", checkable->GetMaxCheckAttempts());
	fields->Set("host", host->GetName());

	if (service) {
		fields->Set("service", service->GetShortName());
		fields->Set("state", service->GetState());
		fields->Set("last_state", service->GetLastState());
		fields->Set("last_hard_state", service->GetLastHardState());
	} else {
		fields->Set("state", host->GetState());
		fields->Set("last_state", host->GetLastState());
		fields->Set("last_hard_state", host->GetLastHardState());
	}

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		fields->Set("check_command", commandObj->GetName());

	double ts = Utility::GetTime();

	if (cr) {
		AddCheckResult(fields, checkable, cr);
		ts = cr->GetExecutionEnd();
	}

	Enqueue("statechange", fields, ts);
}

void ElasticWriter::NotificationSentToAllUsersHandler(const Notification::Ptr& notification,
    const Checkable::Ptr& checkable, const std::set<User::Ptr>& users, NotificationType type,
    const CheckResult::Ptr& cr, const String& author, const String& text)
{
	m_WorkQueue.Enqueue(boost::bind(&ElasticWriter::NotificationSentToAllUsersHandlerInternal, this,
	    notification, checkable, users, type, cr, author, text));
}

void ElasticWriter::NotificationSentToAllUsersHandlerInternal(const Notification::Ptr& notification,
    const Checkable::Ptr& checkable, const std::set<User::Ptr>& users, NotificationType type,
    const CheckResult::Ptr& cr, const String& author, const String& text)
{
	AssertOnWorkQueue();

	CONTEXT("Elasticwriter processing notification to all users '" + checkable->GetName() + "'");

	Log(LogDebug, "ElasticWriter")
	    << "Processing notification for '" << checkable->GetName() << "'";

	Host::Ptr host;
	Service::Ptr service;
	tie(host, service) = GetHostService(checkable);

	String notificationTypeString = Notification::NotificationTypeToString(type);

	Dictionary::Ptr fields = new Dictionary();

	if (service) {
		fields->Set("service", service->GetShortName());
		fields->Set("state", service->GetState());
		fields->Set("last_state", service->GetLastState());
		fields->Set("last_hard_state", service->GetLastHardState());
	} else {
		fields->Set("state", host->GetState());
		fields->Set("last_state", host->GetLastState());
		fields->Set("last_hard_state", host->GetLastHardState());
	}

	fields->Set("host", host->GetName());

	Array::Ptr userNames = new Array();

	for (const User::Ptr& user : users) {
		userNames->Add(user->GetName());
	}

	fields->Set("users", userNames);
	fields->Set("notification_type", notificationTypeString);
	fields->Set("author", author);
	fields->Set("text", text);

	CheckCommand::Ptr commandObj = checkable->GetCheckCommand();

	if (commandObj)
		fields->Set("check_command", commandObj->GetName());

	double ts = Utility::GetTime();

	if (cr) {
		AddCheckResult(fields, checkable, cr);
		ts = cr->GetExecutionEnd();
	}

	Enqueue("notification", fields, ts);
}

void ElasticWriter::Enqueue(String type, const Dictionary::Ptr& fields, double ts)
{
	// Atomically buffer the data point
	boost::mutex::scoped_lock lock(m_DataBufferMutex);

	// Every payload needs a line describing the index above
	// We do it this way to avoid problems with a near full queue

	//TODO:
	/* The date format must match the default dynamic date detection
	 * pattern in indexes. This enables applications like Kibana to
	 * detect a qualified timestamp index for time-series data.
	 * More details here: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html#date-detection
	 */
	//String formattedNow = Utility::FormatDateTime("%Y-%m-%d %H:%M:%S %Z", ts);

	String eventType = m_EventPrefix + type;

	//TODO
	/* Store the timestamp as milli-seconds-since-epoch.
	 * Details: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
	 */
	fields->Set("@timestamp", static_cast<unsigned int>(ts * 1000));
	fields->Set("timestamp", static_cast<unsigned int>(ts * 1000));
	fields->Set("type", eventType);

	String data;

	data += "{ \"index\" : { \"_type\" : \"" + eventType + "\" } }\n";
	//data = "{ \"mappings\": { \"_default_\": \"properties\": { \"timestamp\": { \"type\": \"date\" } } } }\n";
	data += JsonEncode(fields);

	m_DataBuffer.push_back(data);

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

	SendRequest(body);
}

void ElasticWriter::SendRequest(const String& body)
{
	Url::Ptr url = new Url();
	url->SetScheme("http");
	url->SetHost(GetHost());
	url->SetPort(GetPort());

	std::vector<String> path;
	path.push_back(GetIndex() + "-" + Utility::FormatDateTime("%Y.%m.%d", Utility::GetTime()));
	path.push_back("_bulk");
	url->SetPath(path);

	Stream::Ptr stream = Connect();
	HttpRequest req(stream);
	req.AddHeader("Accept", "application/json");
	req.AddHeader("Content-Type", "application/json");
	req.RequestMethod = "POST";
	req.RequestUrl = url;

#ifdef I2_DEBUG /* I2_DEBUG */
	Log(LogInformation, "ElasticWriter")
	    << "Sending body: " << body;
#endif /* I2_DEBUG */

	try {
		req.WriteBody(body.CStr(), body.GetLength());
		req.Finish();
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
			<< "Cannot write to HTTP API on host '" << GetHost() << "' port '" << GetPort() << "'.";
		throw ex;
	}

	HttpResponse resp(stream, req);
	StreamReadContext context;

	try {
		resp.Parse(context, true);
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
			<< "Cannot read from HTTP API on host '" << GetHost() << "' port '" << GetPort() << "'.";
		throw ex;
	}

	if (resp.StatusCode > 299) {
		Log(LogWarning, "ElasticWriter")
		    << "Unexpected response code " << resp.StatusCode;

		// Finish parsing the headers and body
		while (!resp.Complete)
			resp.Parse(context, true);

		String contentType = resp.Headers->Get("content-type");
		if (contentType != "application/json") {
			Log(LogWarning, "ElasticWriter")
			    << "Unexpected Content-Type: " << contentType;
			return;
		}

		size_t responseSize = resp.GetBodySize();
		boost::scoped_array<char> buffer(new char[responseSize + 1]);
		resp.ReadBody(buffer.get(), responseSize);
		buffer.get()[responseSize] = '\0';

		Dictionary::Ptr jsonResponse;
		try {
			jsonResponse = JsonDecode(buffer.get());
		} catch (...) {
			Log(LogWarning, "ElasticWriter")
			    << "Unable to parse JSON response:\n" << buffer.get();
			return;
		}

		String error = jsonResponse->Get("error");

		Log(LogCritical, "ElasticWriter")
		    << "Elasticsearch error message:\n" << error;
	}
}

Stream::Ptr ElasticWriter::Connect(void)
{
	TcpSocket::Ptr socket = new TcpSocket();

	Log(LogNotice, "ElasticWriter")
	    << "Connecting to Elasticsearch on host '" << GetHost() << "' port '" << GetPort() << "'.";

	try {
		socket->Connect(GetHost(), GetPort());
	} catch (const std::exception& ex) {
		Log(LogWarning, "ElasticWriter")
		    << "Can't connect to Elasticsearch on host '" << GetHost() << "' port '" << GetPort() << "'.";
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
