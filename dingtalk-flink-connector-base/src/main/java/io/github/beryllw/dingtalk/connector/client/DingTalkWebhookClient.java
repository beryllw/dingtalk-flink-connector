package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DingTalk Webhook client with HMAC-SHA256 signature and token bucket rate limiting.
 * Webhook mode is limited to 20 messages per minute by DingTalk.
 */
public class DingTalkWebhookClient implements DingTalkClient {

    private static final Logger LOG = LoggerFactory.getLogger(DingTalkWebhookClient.class);

    // Token bucket: 20 tokens max, refill 1 token every 3 seconds
    private static final int MAX_TOKENS = 20;
    private static final long REFILL_INTERVAL_MS = 3000;

    private final String webhookUrl;
    private final String secret;
    private final int connectionTimeoutMs;

    private final AtomicInteger tokens = new AtomicInteger(MAX_TOKENS);
    private final AtomicLong lastRefillTime = new AtomicLong(System.currentTimeMillis());

    public DingTalkWebhookClient(DingTalkSinkOptions options) {
        this.webhookUrl = options.getWebhook();
        this.secret = options.getSecret();
        this.connectionTimeoutMs = options.getConnectionTimeoutMs();
    }

    @Override
    public String send(String payload) throws IOException {
        waitForToken();
        String url = buildSignedUrl();
        return doPost(url, payload);
    }

    @Override
    public void close() {
        // No resources to release for webhook client
    }

    private void waitForToken() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRefillTime.get();
        long tokensToAdd = elapsed / REFILL_INTERVAL_MS;
        if (tokensToAdd > 0) {
            lastRefillTime.set(now);
            tokens.updateAndGet(current -> Math.min(MAX_TOKENS, current + (int) tokensToAdd));
        }

        // If no tokens available, wait
        if (tokens.decrementAndGet() < 0) {
            try {
                Thread.sleep(REFILL_INTERVAL_MS);
                tokens.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for rate limit token", e);
            }
        }
    }

    private String buildSignedUrl() {
        if (secret == null || secret.isEmpty()) {
            return webhookUrl;
        }

        try {
            long timestamp = System.currentTimeMillis();
            String stringToSign = timestamp + "\n" + secret;

            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] signData = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
            String sign = Base64.getEncoder().encodeToString(signData);

            return webhookUrl + "&timestamp=" + timestamp + "&sign=" +
                    URLEncoder.encode(sign, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            LOG.warn("Failed to sign webhook URL, sending without signature", e);
            return webhookUrl;
        }
    }

    private String doPost(String urlString, String payload) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(connectionTimeoutMs);
        conn.setReadTimeout(connectionTimeoutMs);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        String response;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        responseCode == 200 ? conn.getInputStream() : conn.getErrorStream(),
                        StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            response = sb.toString();
        }

        if (responseCode != 200) {
            throw new IOException("Webhook request failed with code " + responseCode + ": " + response);
        }

        LOG.debug("Webhook response: {}", response);
        return response;
    }
}
