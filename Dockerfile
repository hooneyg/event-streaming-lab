# ☕ Stage 1: Build (Java 21 LTS)
FROM eclipse-temurin:21-jdk-jammy AS build

WORKDIR /app

# Gradle Wrapper 및 설정 파일 복사
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .

# 의존성 미리 다운로드
RUN ./gradlew dependencies --no-daemon

# 소스 코드 복사 및 빌드
COPY src src
RUN ./gradlew build -x test --no-daemon

# 🚀 Stage 2: Run
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 빌드 결과물만 복사
COPY --from=build /app/build/libs/*.jar app.jar

# 컨테이너 실행
ENTRYPOINT ["java", "-jar", "app.jar"]

EXPOSE 8080
