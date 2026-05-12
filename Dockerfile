# ☕ Stage 1: Build (Java 21 LTS) - 빌드 환경 구성
FROM eclipse-temurin:21-jdk-jammy AS build

# 작업 디렉토리 설정
WORKDIR /app

# Gradle Wrapper 및 설정 파일 복사 (캐시 효율성을 위해 의존성 관련 파일 먼저 복사)
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .

# 의존성 미리 다운로드 (소스 코드 변경 시에도 의존성 레이어 캐시 활용)
RUN ./gradlew dependencies --no-daemon

# 전체 소스 코드 복사 및 빌드 (테스트는 인프라 의존성으로 인해 빌드 시 제외)
COPY src src
RUN ./gradlew build -x test --no-daemon

# 🚀 Stage 2: Run - 실행 환경 구성 (경량 JRE 이미지 사용)
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 빌드 결과물(jar)만 복사하여 이미지 크기 최소화
COPY --from=build /app/build/libs/*.jar app.jar

# 컨테이너 실행 명령 (Spring Boot 앱 구동)
ENTRYPOINT ["java", "-jar", "app.jar"]

# 애플리케이션 포트 노출
EXPOSE 8080
