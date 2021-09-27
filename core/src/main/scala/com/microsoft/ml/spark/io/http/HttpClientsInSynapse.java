package com.microsoft.ml.spark.io.http;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.xbill.DNS.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HttpClientsInSynapse {

    private static SimpleResolver dnsServer;

    DnsResolver dnsResolver = new SystemDefaultDnsResolver() {
        @Override
        public InetAddress[] resolve(final String host) throws UnknownHostException {
            if (host.endsWith("cognitiveservices.azure.com")) {
            /* If we match the host we're trying to talk to,
               return the IP address we want, not what is in DNS */
                String ip;
                try {
                    ip = resolveHostWrapper(host);
                    return new InetAddress[]{InetAddress.getByName(ip)};
                } catch (TextParseException e) {
                    e.printStackTrace();
                    return super.resolve(host);
                }
            } else {
                /* Else, resolve it as we would normally */
                return super.resolve(host);
            }
        }
    };

    /* HttpClientConnectionManager allows us to use custom DnsResolver */
    BasicHttpClientConnectionManager connManager = new BasicHttpClientConnectionManager(
    /* We're forced to create a SocketFactory Registry.  Passing null
       doesn't force a default Registry, so we re-invent the wheel. */
            RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", SSLConnectionSocketFactory.getSocketFactory())
                    .build(),
            null, /* Default ConnectionFactory */
            null, /* Default SchemePortResolver */
            dnsResolver  /* Our DnsResolver */
    );

    Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory())
            .register("https", SSLConnectionSocketFactory.getSocketFactory())
            .build();


    CloseableHttpClient httpClient;

    public HttpClientsInSynapse() {
        try {
            dnsServer = new SimpleResolver("168.63.129.16");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            dnsServer = null;
        }

        PoolingHttpClientConnectionManager poolConnMgr = new PoolingHttpClientConnectionManager(
                socketFactoryRegistry,
                dnsResolver);

        poolConnMgr.setDefaultMaxPerRoute(Integer.MAX_VALUE);

        /* build HttpClient that will use our DnsResolver */
        httpClient = HttpClientBuilder.create()
                .setConnectionManager(poolConnMgr)
                .build();
    }


    private static Record[] resolveHost(String host) throws TextParseException {
        Lookup lookup = new Lookup(host);
        lookup.setResolver(dnsServer);
        return lookup.run();
    }

    private static String resolveIP(Record[] records) {
        String[] ip = records[0].toString().split("\t");
        return ip[ip.length - 1];
    }

    private static String resolveHostWrapper(String host) throws TextParseException {
        Lookup lookup = new Lookup(host);
        Record[] res = lookup.run();
        if (res == null) {
            for (Name alia : lookup.getAliases()) {
                Record[] resolveResults = resolveHost(alia.toString());
                if (resolveResults != null) {
                    return resolveIP(resolveResults);
                }
            }
            return "";
        }
        return resolveIP(res);
    }
}