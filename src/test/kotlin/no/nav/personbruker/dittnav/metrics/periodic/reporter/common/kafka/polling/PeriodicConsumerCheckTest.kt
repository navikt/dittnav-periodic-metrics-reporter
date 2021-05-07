package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling


import io.mockk.*
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup
import org.amshove.kluent.`should be empty`
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class PeriodicConsumerCheckTest {

    private val appContext = mockk<ApplicationContext>(relaxed = true)
    private val periodicConsumerCheck = PeriodicConsumerCheck(appContext)

    @BeforeEach
    fun resetMocks() {
        mockkObject(KafkaConsumerSetup)
        coEvery { KafkaConsumerSetup.restartConsumers(appContext) } returns Unit
        coEvery { KafkaConsumerSetup.stopAllKafkaConsumers(appContext) } returns Unit
        coEvery { appContext.reinitializeConsumers() } returns Unit
        coEvery { KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumers(appContext) } returns Unit
    }

    @AfterAll
    fun cleanUp() {
        unmockkAll()
    }

    @Test
    fun `Skal returnere en liste med konsumenter som har stoppet aa polle`() {
        coEvery { appContext.beskjedCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.doneCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.oppgaveCountConsumer.getNumberOfFailedCounts() } returns 0

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns true
        coEvery { appContext.doneCountConsumer.isStopped() } returns true
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStopped().size `should be equal to` 2
        }
    }

    @Test
    fun `Skal returnere en tom liste hvis alle konsumenter kjorer som normalt`() {
        coEvery { appContext.beskjedCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.doneCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.oppgaveCountConsumer.getNumberOfFailedCounts() } returns 0

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns false
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStopped().`should be empty`()
        }
    }

    @Test
    fun `Skal kalle paa restartConsumers hvis en eller flere konsumere har sluttet aa kjore`() {
        coEvery { appContext.beskjedCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.doneCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.oppgaveCountConsumer.getNumberOfFailedCounts() } returns 0

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns true
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns true

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }

        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumers(appContext) }
        confirmVerified(KafkaConsumerSetup)
    }

    @Test
    fun `Skal ikke restarte konsumer hvis alle kafka-konsumerne kjorer`() {
        coEvery { appContext.beskjedCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.doneCountConsumer.getNumberOfFailedCounts() } returns 0
        coEvery { appContext.oppgaveCountConsumer.getNumberOfFailedCounts() } returns 0

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns false
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }

        coVerify(exactly = 0) { KafkaConsumerSetup.restartConsumers(appContext) }
        confirmVerified(KafkaConsumerSetup)
    }

    @Test
    fun `Skal restarte beskjed konsumer hvis den ikke har klart aa telle eventer`() {
        every { appContext.environment.maxFailedCounts } returns 5
        coEvery { appContext.beskjedCountConsumer.getNumberOfFailedCounts() } returns 6

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns false
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }

        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumers(appContext) }
        confirmVerified(KafkaConsumerSetup)
    }

    @Test
    fun `Skal restarte done konsumer hvis den ikke har klart aa telle eventer`() {
        every { appContext.environment.maxFailedCounts } returns 5
        coEvery { appContext.doneCountConsumer.getNumberOfFailedCounts() } returns 6

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns false
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }

        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumers(appContext) }
        confirmVerified(KafkaConsumerSetup)
    }

    @Test
    fun `Skal restarte oppgave konsumer hvis den ikke har klart aa telle eventer`() {
        every { appContext.environment.maxFailedCounts } returns 5
        coEvery { appContext.oppgaveCountConsumer.getNumberOfFailedCounts() } returns 6

        coEvery { appContext.beskjedCountConsumer.isStopped() } returns false
        coEvery { appContext.doneCountConsumer.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumer.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }

        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumers(appContext) }
        confirmVerified(KafkaConsumerSetup)
    }
}