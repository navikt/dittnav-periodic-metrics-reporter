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
        coEvery { KafkaConsumerSetup.restartConsumersOnPrem(appContext) } returns Unit
        coEvery { KafkaConsumerSetup.restartConsumersAiven(appContext) } returns Unit
        coEvery { KafkaConsumerSetup.stopAllKafkaConsumersOnPrem(appContext) } returns Unit
        coEvery { KafkaConsumerSetup.stopAllKafkaConsumersAiven(appContext) } returns Unit
        coEvery { appContext.reinitializeConsumersOnPrem() } returns Unit
        coEvery { appContext.reinitializeConsumersAiven() } returns Unit
        coEvery { KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumersOnPrem(appContext) } returns Unit
        coEvery { KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumersAiven(appContext) } returns Unit
    }

    @AfterAll
    fun cleanUp() {
        unmockkAll()
    }

    @Test
    fun `Skal returnere en liste med konsumere som har stoppet aa polle on-prem`() {
        coEvery { appContext.beskjedCountConsumerOnPrem.isStopped() } returns true
        coEvery { appContext.doneCountConsumerOnPrem.isStopped() } returns true
        coEvery { appContext.oppgaveCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerOnPrem.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStoppedOnPrem().size `should be equal to` 2
        }
    }

    @Test
    fun `Skal returnere en liste med konsumere som har stoppet aa polle paa Aiven`() {
        coEvery { appContext.beskjedCountConsumerAiven.isStopped() } returns true
        coEvery { appContext.doneCountConsumerAiven.isStopped() } returns true
        coEvery { appContext.oppgaveCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerAiven.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStoppedAiven().size `should be equal to` 2
        }
    }

    @Test
    fun `Skal returnere en tom liste hvis alle konsumere kjorer som normalt on-prem`() {
        coEvery { appContext.beskjedCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.doneCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerOnPrem.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStoppedOnPrem().`should be empty`()
        }
    }

    @Test
    fun `Skal returnere en tom liste hvis alle konsumere kjorer som normalt paa Aiven`() {
        coEvery { appContext.beskjedCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.doneCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerAiven.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.getConsumersThatHaveStoppedAiven().`should be empty`()
        }
    }

    @Test
    fun `Skal kalle paa restartConsumers hvis en eller flere konsumere har sluttet aa kjore on-prem`() {
        coEvery { appContext.beskjedCountConsumerOnPrem.isStopped() } returns true
        coEvery { appContext.doneCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerOnPrem.isStopped() } returns true
        coEvery { appContext.statusoppdateringCountConsumerOnPrem.isStopped() } returns true

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }
        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumersOnPrem(appContext) }
    }

    @Test
    fun `Skal kalle paa restartConsumers hvis en eller flere konsumere har sluttet aa kjore paa Aiven`() {
        coEvery { appContext.beskjedCountConsumerAiven.isStopped() } returns true
        coEvery { appContext.doneCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerAiven.isStopped() } returns true
        coEvery { appContext.statusoppdateringCountConsumerAiven.isStopped() } returns true

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }
        coVerify(exactly = 1) { KafkaConsumerSetup.restartConsumersAiven(appContext) }
    }

    @Test
    fun `Skal ikke restarte konsumer hvis alle kafka-konsumerne kjorer on-prem`() {
        coEvery { appContext.beskjedCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.doneCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerOnPrem.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerOnPrem.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }
        coVerify(exactly = 0) { KafkaConsumerSetup.restartConsumersOnPrem(appContext) }
    }

    @Test
    fun `Skal ikke restarte konsumer hvis alle kafka-konsumerne kjorer paa Aiven`() {
        coEvery { appContext.beskjedCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.doneCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.oppgaveCountConsumerAiven.isStopped() } returns false
        coEvery { appContext.statusoppdateringCountConsumerAiven.isStopped() } returns false

        runBlocking {
            periodicConsumerCheck.checkIfConsumersAreRunningAndRestartIfNot()
        }
        coVerify(exactly = 0) { KafkaConsumerSetup.restartConsumersAiven(appContext) }
    }
}
