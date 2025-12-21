FROM debian:trixie-20230612

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | gpg --dearmor -o /usr/share/keyrings/cloudflare-main.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" \
    | tee /etc/apt/sources.list.d/cloudflared.list

RUN apt-get update && apt-get install -y cloudflared && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /apps/cloudflared

# copy the entrypoint
COPY apps/cloudflared/entrypoint.sh /entrypoint.sh
RUN ls && chmod +x /entrypoint.sh