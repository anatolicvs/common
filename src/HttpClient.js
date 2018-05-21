class HttpClient {
    request(method, path, requestContent) {

        return new Promise((resolve, reject) => {

            let requestPayload;

            if (requestContent === undefined) {
                // ok
            }
            else {

                requestPayload = Buffer.from(JSON.stringify(requestContent), "utf8");
            }

            const urlInfo = parseUrl(
                `${this.baseUrl}${path}`
            );

            const lib = urlInfo.protocol === "https:" ? https : http;

            const options = {
                protocol: urlInfo.protocol,
                hostname: urlInfo.hostname,
                port: urlInfo.port,
                method,
                path: urlInfo.path
            };

            if (requestPayload === undefined) {
                // ok
            }
            else {
                options.headers = {
                    "Content-Type": "application/json; charset=utf-8",
                    "Content-Length": `${requestPayload.length}`
                };
            }

            const request = lib.request(
                options
            );

            request.once("response", response => {

                const chunks = [];

                response.on("data", chunk => {
                    chunks.push(chunk);
                });

                response.once("error", error => {
                    reject(error);
                });

                response.once("end", () => {

                    // if (response.statusCode === 200) {
                    // 	// ok
                    // }
                    // else {
                    // 	reject(new Error(`http-${response.statusCode}`));
                    // 	return;
                    // }

                    if (0 < chunks.length) {

                        const responseContent = JSON.parse(Buffer.concat(chunks).toString("utf8"));
                        resolve(responseContent);
                    }
                    else {
                        resolve();
                    }
                });
            });

            request.once("error", error => {
                reject(error);
            });

            if (requestPayload === undefined) {
                request.end();
            }
            else {
                request.end(
                    requestPayload
                );
            }
        });
    }

    get(path, content) {

        return this.request("GET", path, content);
    }

    post(path, content) {

        return this.request("POST", path, content);
    }

    delete(path) {

        return this.request("DELETE", path);
    }

    put(path, content) {

        return this.request("PUT", path, content);
    }
}

HttpClient.prototype.baseUrl = null;

module.exports = {
    HttpClient
}