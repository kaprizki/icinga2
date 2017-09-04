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

#include "remote/jsonrpcconnection.hpp"
#include "remote/apilistener.hpp"
#include "remote/apifunction.hpp"
#include "remote/jsonrpc.hpp"
#include "base/configtype.hpp"
#include "base/objectlock.hpp"
#include "base/utility.hpp"
#include "base/logger.hpp"
#include "base/exception.hpp"
#include "base/convert.hpp"
#include <boost/thread/once.hpp>
#include <boost/regex.hpp>
#include <fstream>

using namespace icinga;

static Value RequestCertificateHandler(const MessageOrigin::Ptr& origin, const Dictionary::Ptr& params);
REGISTER_APIFUNCTION(RequestCertificate, pki, &RequestCertificateHandler);
static Value UpdateCertificateHandler(const MessageOrigin::Ptr& origin, const Dictionary::Ptr& params);
REGISTER_APIFUNCTION(UpdateCertificate, pki, &UpdateCertificateHandler);

Value RequestCertificateHandler(const MessageOrigin::Ptr& origin, const Dictionary::Ptr& params)
{
	if (!params)
		return Empty;

	String certText = params->Get("cert_request");

	boost::shared_ptr<X509> cert;

	Dictionary::Ptr result = new Dictionary();

	if (certText.IsEmpty())
		cert = origin->FromClient->GetStream()->GetPeerCertificate();
	else
		cert = StringToCertificate(certText);

	unsigned int n;
	unsigned char digest[EVP_MAX_MD_SIZE];

	if (!X509_digest(cert.get(), EVP_sha256(), digest, &n)) {
		result->Set("status_code", 1);
		result->Set("error", "Could not calculate fingerprint for the X509 certificate.");
		return result;
	}

	char certFingerprint[EVP_MAX_MD_SIZE*2+1];
	for (unsigned int i = 0; i < n; i++)
		sprintf(certFingerprint + 2 * i, "%02x", digest[i]);

	result->Set("fingerprint_request", certFingerprint);

	String requestDir = Application::GetLocalStateDir() + "/lib/icinga2/pki-requests";
	String requestPath = requestDir + "/" + certFingerprint + ".json";

	ApiListener::Ptr listener = ApiListener::GetInstance();

	boost::shared_ptr<X509> cacert = GetX509Certificate(listener->GetCaPath());
	result->Set("ca", CertificateToString(cacert));

	if (Utility::PathExists(requestPath)) {
		Dictionary::Ptr request = Utility::LoadJsonFile(requestPath);

		String certResponse = request->Get("cert_response");

		if (!certResponse.IsEmpty()) {
			result->Set("cert", certResponse);
			result->Set("status_code", 0);

			Dictionary::Ptr message = new Dictionary();
			message->Set("jsonrpc", "2.0");
			message->Set("method", "pki::UpdateCertificate");
			message->Set("params", result);
			JsonRpc::SendMessage(origin->FromClient->GetStream(), message);

			return result;
		}
	}

	boost::shared_ptr<X509> newcert;
	boost::shared_ptr<EVP_PKEY> pubkey;
	X509_NAME *subject;
	Dictionary::Ptr message;

	if (!Utility::PathExists(GetIcingaCADir() + "/ca.key"))
		goto delayed_request;

	if (!VerifyCertificate(cacert, cert)) {
		String salt = listener->GetTicketSalt();

		String ticket = params->Get("ticket");

		if (salt.IsEmpty() || ticket.IsEmpty())
			goto delayed_request;

		String realTicket = PBKDF2_SHA1(origin->FromClient->GetIdentity(), salt, 50000);

		if (ticket != realTicket) {
			result->Set("status_code", 1);
			result->Set("error", "Invalid ticket.");
			return result;
		}
	} else {
		time_t renewalStart;
		time(&renewalStart);
		renewalStart += 30 * 24 * 60 * 60;

		if (X509_cmp_time(X509_get_notAfter(cert.get()), &renewalStart)) {
			result->Set("status_code", 1);
			result->Set("error", "The certificate cannot be renewed yet.");
			return result;
		}
	}


	pubkey = boost::shared_ptr<EVP_PKEY>(X509_get_pubkey(cert.get()), EVP_PKEY_free);
	subject = X509_get_subject_name(cert.get());

	newcert = CreateCertIcingaCA(pubkey.get(), subject);

	/* verify that the new cert matches the CA we're using for the ApiListener;
	 * this ensures that the CA we have in /var/lib/icinga2/ca matches the one
	 * we're using for cluster connections (there's no point in sending a client
	 * a certificate it wouldn't be able to use to connect to us anyway) */
	if (!VerifyCertificate(cacert, newcert)) {
		Log(LogWarning, "JsonRpcConnection")
		    << "The CA in '" << listener->GetCaPath() << "' does not match the CA which Icinga uses "
		    << "for its own cluster connections. This is most likely a configuration problem.";
		goto delayed_request;
	}

	result->Set("cert", CertificateToString(newcert));

	result->Set("status_code", 0);

	message = new Dictionary();
	message->Set("jsonrpc", "2.0");
	message->Set("method", "pki::UpdateCertificate");
	message->Set("params", result);
	JsonRpc::SendMessage(origin->FromClient->GetStream(), message);

	return result;

delayed_request:
	Utility::MkDirP(requestDir, 0700);

	Dictionary::Ptr request = new Dictionary();
	request->Set("cert_request", CertificateToString(cert));
	request->Set("ticket", params->Get("ticket"));

	Utility::SaveJsonFile(requestPath, 0600, request);

	JsonRpcConnection::SendCertificateRequest(JsonRpcConnection::Ptr(), origin, requestPath);

	result->Set("status_code", 2);
	result->Set("error", "Certificate request is pending. Waiting for approval from the parent Icinga instance.");
	return result;
}

void JsonRpcConnection::SendCertificateRequest(const JsonRpcConnection::Ptr& aclient, const MessageOrigin::Ptr& origin, const String& path)
{
	Dictionary::Ptr message = new Dictionary();
	message->Set("jsonrpc", "2.0");
	message->Set("method", "pki::RequestCertificate");

	ApiListener::Ptr listener = ApiListener::GetInstance();

	if (!listener)
		return;

	Dictionary::Ptr params = new Dictionary();
	message->Set("params", params);

	/* path is empty if this is our own request */
	if (path.IsEmpty()) {
		String ticketPath = Application::GetLocalStateDir() + "/lib/icinga2/pki/ticket";

		std::ifstream fp(ticketPath.CStr());
		String ticket((std::istreambuf_iterator<char>(fp)), std::istreambuf_iterator<char>());
		fp.close();

		params->Set("ticket", ticket);
	} else {
		Dictionary::Ptr request = Utility::LoadJsonFile(path);

		if (request->Contains("cert_response"))
			return;

		params->Set("cert_request", request->Get("cert_request"));
		params->Set("ticket", request->Get("ticket"));
	}

	if (aclient)
		JsonRpc::SendMessage(aclient->GetStream(), message);
	else
		listener->RelayMessage(origin, Zone::GetLocalZone(), message, false);
}

Value UpdateCertificateHandler(const MessageOrigin::Ptr& origin, const Dictionary::Ptr& params)
{
	if (origin->FromZone && !Zone::GetLocalZone()->IsChildOf(origin->FromZone)) {
		Log(LogWarning, "ClusterEvents")
		    << "Discarding 'update certificate' message from '" << origin->FromClient->GetIdentity() << "': Invalid endpoint origin (client not allowed).";

		return Empty;
	}

	Log(LogWarning, "JsonRpcConnection")
	    << params->ToString();

	String ca = params->Get("ca");
	String cert = params->Get("cert");

	ApiListener::Ptr listener = ApiListener::GetInstance();

	if (!listener)
		return Empty;

	boost::shared_ptr<X509> oldCert = GetX509Certificate(listener->GetCertPath());
	boost::shared_ptr<X509> newCert = StringToCertificate(cert);

	Log(LogWarning, "JsonRpcConnection")
	    << "Received certificate update message for CN '" << GetCertificateCN(newCert) << "'";

	/* check if this is a certificate update for a subordinate instance */
	boost::shared_ptr<EVP_PKEY> oldKey = boost::shared_ptr<EVP_PKEY>(X509_get_pubkey(oldCert.get()), EVP_PKEY_free);
	boost::shared_ptr<EVP_PKEY> newKey = boost::shared_ptr<EVP_PKEY>(X509_get_pubkey(newCert.get()), EVP_PKEY_free);

	if (X509_NAME_cmp(X509_get_subject_name(oldCert.get()), X509_get_subject_name(newCert.get())) != 0 ||
	    EVP_PKEY_cmp(oldKey.get(), newKey.get()) != 1) {
		String certFingerprint = params->Get("fingerprint_request");

		boost::regex expr("^[0-9a-f]+$");

		if (!boost::regex_match(certFingerprint.GetData(), expr)) {
			Log(LogWarning, "JsonRpcConnection")
			    << "Endpoint '" << origin->FromClient->GetIdentity() << "' sent an invalid certificate fingerprint: " << certFingerprint;
			return Empty;
		}

		String requestDir = Application::GetLocalStateDir() + "/lib/icinga2/pki-requests";
		String requestPath = requestDir + "/" + certFingerprint + ".json";

		std::cout << requestPath << "\n";

		if (Utility::PathExists(requestPath)) {
			Log(LogWarning, "JsonRpcConnection")
			    << "Saved certificate update for CN '" << GetCertificateCN(newCert) << "'";

			Dictionary::Ptr request = Utility::LoadJsonFile(requestPath);
			request->Set("cert_response", cert);
			Utility::SaveJsonFile(requestPath, 0644, request);
		}

		return Empty;
	}

	String caPath = listener->GetCaPath();

	std::fstream cafp;
	String tempCaPath = Utility::CreateTempFile(caPath + ".XXXXXX", 0644, cafp);
	cafp << ca;
	cafp.close();

#ifdef _WIN32
	_unlink(caPath.CStr());
#endif /* _WIN32 */

	if (rename(tempCaPath.CStr(), caPath.CStr()) < 0) {
		BOOST_THROW_EXCEPTION(posix_error()
		    << boost::errinfo_api_function("rename")
		    << boost::errinfo_errno(errno)
		    << boost::errinfo_file_name(tempCaPath));
	}

	String certPath = listener->GetCertPath();

	std::fstream certfp;
	String tempCertPath = Utility::CreateTempFile(certPath + ".XXXXXX", 0644, certfp);
	certfp << cert;
	certfp.close();

#ifdef _WIN32
	_unlink(certPath.CStr());
#endif /* _WIN32 */

	if (rename(tempCertPath.CStr(), certPath.CStr()) < 0) {
		BOOST_THROW_EXCEPTION(posix_error()
		    << boost::errinfo_api_function("rename")
		    << boost::errinfo_errno(errno)
		    << boost::errinfo_file_name(tempCertPath));
	}

	String ticketPath = Application::GetLocalStateDir() + "/lib/icinga2/pki/ticket";

	if (unlink(ticketPath.CStr()) < 0 && errno != ENOENT) {
		BOOST_THROW_EXCEPTION(posix_error()
		    << boost::errinfo_api_function("unlink")
		    << boost::errinfo_errno(errno)
		    << boost::errinfo_file_name(ticketPath));
	}

	Log(LogInformation, "JsonRpcConnection", "Updating the client certificate for the ApiListener object");
	listener->UpdateSSLContext();

	return Empty;
}