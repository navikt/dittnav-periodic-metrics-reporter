import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val prometheusVersion = "0.6.0"
val ktorVersion = "1.1.3"
val junitVersion = "5.4.1"
val kafkaVersion = "2.3.0"
val confluentVersion = "5.3.0"
val brukernotifikasjonSchemaVersion = "1.2019.08.22-13.39-d4273d3247a3"
val logstashVersion = 5.2
val logbackVersion = "1.2.3"
val vaultJdbcVersion = "1.3.1"
val flywayVersion = "5.2.4"
val hikariCPVersion = "3.2.0"
val postgresVersion = "42.2.5"
val h2Version = "1.4.199"
val assertJVersion = "3.12.2"
val kafkaEmvededVersion = "2.2.1"
val kluentVersion = "1.52"
val kafkaEmbeddedEnvVersion = "2.1.1"

plugins {
    // Apply the Kotlin JVM plugin to add support for Kotlin on the JVM.
    kotlin("jvm").version("1.3.31")
    kotlin("plugin.allopen").version("1.3.31")

    id("org.flywaydb.flyway") version("5.2.4")

    // Apply the application plugin to add support for building a CLI application.
    application
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

repositories {
    jcenter()
    maven("http://packages.confluent.io/maven")
    mavenLocal()
}

sourceSets {
    create("intTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

val intTestImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
configurations["intTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    compile("no.nav:vault-jdbc:$vaultJdbcVersion")
    compile("com.zaxxer:HikariCP:$hikariCPVersion")
    compile("org.postgresql:postgresql:$postgresVersion")
    compile("org.flywaydb:flyway-core:$flywayVersion")
    compile("ch.qos.logback:logback-classic:$logbackVersion")
    compile("net.logstash.logback:logstash-logback-encoder:$logstashVersion")
    compile("io.prometheus:simpleclient_common:$prometheusVersion")
    compile("io.prometheus:simpleclient_hotspot:$prometheusVersion")
    compile("io.ktor:ktor-server-netty:$ktorVersion")
    compile("org.apache.kafka:kafka-clients:$kafkaVersion")
    compile("io.confluent:kafka-avro-serializer:$confluentVersion")
    compile("no.nav:brukernotifikasjon-schemas:$brukernotifikasjonSchemaVersion")
    testCompile("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testCompile(kotlin("test-junit5"))
    testImplementation("no.nav:kafka-embedded-env:$kafkaEmbeddedEnvVersion")
    testImplementation("org.apache.kafka:kafka_2.12:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    testImplementation("io.confluent:kafka-schema-registry:$confluentVersion")
    testImplementation("com.h2database:h2:$h2Version")
    testImplementation("org.amshove.kluent:kluent:$kluentVersion")

    intTestImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
}

application {
    mainClassName = "no.nav.personbruker.dittnav.eventaggregator.AppKt"
}

tasks {
    withType<Jar> {
        manifest {
            attributes["Main-Class"] = application.mainClassName
        }
        from(configurations.runtime.get().map { if (it.isDirectory) it else zipTree(it) })
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    register("runServer", JavaExec::class) {
        main = "no.nav.personbruker.dittnav.eventaggregator.AppKt"
        classpath = sourceSets["main"].runtimeClasspath
    }
}

val integrationTest = task<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["intTest"].output.classesDirs
    classpath = sourceSets["intTest"].runtimeClasspath
    shouldRunAfter("test")
}

tasks.check { dependsOn(integrationTest) }
