module tuthpc.mail;

import std.process;
import std.format;
import std.algorithm;
import std.string;
import std.exception;


void sendMail(string to, string title, string msg)
{
    if("MAILGUN_APIKEY" in environment
    && "MAILGUN_DOMAIN" in environment)
    {
        auto apikey = environment["MAILGUN_APIKEY"];
        auto domain = environment["MAILGUN_DOMAIN"];

        import std.net.curl;
        import std.uri;
        auto http = HTTP("api.mailgun.net");
        http.setAuthentication("api", apikey);
        std.net.curl.post("https://api.mailgun.net/v3/%s/messages".format(domain),
                ["from": "TUTHPCLib <mailgun@%s>".format(domain),
                 "to": to,
                 "subject": title,
                 "text": msg],
                 http
            );
    }
}
