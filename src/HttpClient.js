"use strict";
const { parse: parseUrl } = require("url");
const http = require("http");
const https = require("https");

class HttpClient {

    request(method, path, requestContent) {

        return new Promise((resolve, reject) => {

            let requestBuffer;
            let requestString;

            if (requestContent === undefined) {
                // ok
            }
            else {

                requestString = JSON.stringify(requestContent);
                requestBuffer = Buffer.from(requestString, "utf8");
            }

            const url = `${this.baseUrl}${path}`;
            const urlInfo = parseUrl(
                url
            );

            const lib = urlInfo.protocol === "https:" ? https : http;

            const options = {
                protocol: urlInfo.protocol,
                hostname: urlInfo.hostname,
                port: urlInfo.port,
                method,
                path: urlInfo.path,
                headers: {}
            };

            if (this.authorization === null) {
                // ok
            }
            else {
                options.headers["Authorization"] = this.authorization;
            }

            if (requestBuffer === undefined) {
                // ok
            }
            else {
                options.headers["Content-Type"] = "application/json; charset=utf-8";
                options.headers["Content-Length"] = `${requestBuffer.length}`;
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

                    let responseBuffer;
                    let contentString;
                    let content;

                    if (0 < chunks.length) {

                        responseBuffer = Buffer.concat(
                            chunks
                        );

                        // TODO: if utf8
                        contentString = responseBuffer.toString("utf8");

                        if (this.logResponseContent === true) {

                            this.log.debug(
                                "url:%s, response:%s",
                                url,
                                contentString
                            );
                        }

                        // TODO: if json
                        try {

                            content = JSON.parse(
                                contentString
                            );
                        }
                        catch (error) {

                        }
                    }

                    resolve({
                        statusCode: response.statusCode,
                        responseBuffer,
                        contentString,
                        content
                    });
                });
            });

            request.once("error", error => {
                reject(error);
            });

            if (requestContent === undefined) {

                request.end();
            }
            else {

                if (this.logRequestContent === true) {

                    this.log.debug(
                        "url:%s, request:%s",
                        url,
                        requestString
                    );
                }

                request.end(
                    requestBuffer
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

HttpClient.prototype.log = null;
HttpClient.prototype.baseUrl = null;
HttpClient.prototype.logRequestContent = false;
HttpClient.prototype.logResponseContent = false;
HttpClient.prototype.authorization = null;

module.exports = {
    HttpClient
};

