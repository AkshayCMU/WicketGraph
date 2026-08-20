FROM node:20-alpine AS build

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .

# Vite inlines import.meta.env.* at build time, so the API base has to be a
# build argument. Supplying it as a runtime env var on the nginx stage does
# nothing — the value is already compiled into the bundle.
ARG VITE_API_BASE=http://localhost:8100
ENV VITE_API_BASE=$VITE_API_BASE
RUN npm run build

FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
