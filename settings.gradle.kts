rootProject.name = "rabbitmq-retry-ladder"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("retry-ladder-spring-boot-starter", "demo")
