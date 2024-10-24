FROM node:21.7.0 as build-stage
WORKDIR /app
ENV VITE_APP_ICEBERG_CATALOG_URL=VITE_APP_ICEBERG_CATALOG_URL_PLACEHOLDER
COPY package*.json ./
RUN npm install
COPY ./ .
RUN npm run build

FROM nginx:1.20.1 as production-stage
RUN mkdir /app
COPY --from=build-stage /app/dist /app
COPY nginx.conf /etc/nginx/nginx.conf
COPY /entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
