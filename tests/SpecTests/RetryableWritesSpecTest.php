<?php

namespace MongoDB\Tests\SpecTests;

use Closure;
use Generator;
use MongoDB\Client;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Exception\ConnectionTimeoutException;
use MongoDB\Driver\Monitoring\CommandFailedEvent;
use MongoDB\Driver\Monitoring\CommandStartedEvent;
use MongoDB\Driver\Monitoring\CommandSubscriber;
use MongoDB\Driver\Monitoring\CommandSucceededEvent;
use stdClass;

use function basename;
use function file_get_contents;
use function glob;

/**
 * Retryable writes spec tests.
 *
 * @see https://github.com/mongodb/specifications/tree/master/source/retryable-writes
 * @group serverless
 */
class RetryableWritesSpecTest extends FunctionalTestCase
{
    public const NOT_PRIMARY = 10107;
    public const SHUTDOWN_IN_PROGRESS = 91;

    /**
     * Execute an individual test case from the specification.
     *
     * @dataProvider provideTests
     * @param stdClass $test  Individual "tests[]" document
     * @param array    $runOn Top-level "runOn" array with server requirements
     * @param array    $data  Top-level "data" array to initialize collection
     */
    public function testRetryableWrites(stdClass $test, ?array $runOn, array $data): void
    {
        if ($this->isShardedCluster() && ! $this->isShardedClusterUsingReplicasets()) {
            $this->markTestSkipped('Transaction numbers are only allowed on a replica set member or mongos (PHPC-1415)');
        }

        $useMultipleMongoses = isset($test->useMultipleMongoses) && $test->useMultipleMongoses && $this->isMongos();

        if (isset($runOn)) {
            $this->checkServerRequirements($runOn);
        }

        $context = Context::fromRetryableWrites($test, $this->getDatabaseName(), $this->getCollectionName(), $useMultipleMongoses);
        $this->setContext($context);

        $this->dropTestAndOutcomeCollections();
        $this->insertDataFixtures($data);

        if (isset($test->failPoint)) {
            $this->configureFailPoint($test->failPoint);
        }

        Operation::fromRetryableWrites($test->operation, $test->outcome)->assert($this, $context);

        if (isset($test->outcome->collection->data)) {
            $this->assertOutcomeCollectionData($test->outcome->collection->data);
        }
    }

    public function provideTests()
    {
        $testArgs = [];

        foreach (glob(__DIR__ . '/retryable-writes/*.json') as $filename) {
            $json = $this->decodeJson(file_get_contents($filename));
            $group = basename($filename, '.json');
            $runOn = $json->runOn ?? null;
            $data = $json->data ?? [];

            foreach ($json->tests as $test) {
                $name = $group . ': ' . $test->description;
                $testArgs[$name] = [$test, $runOn, $data];
            }
        }

        return $testArgs;
    }

    /**
     * Prose test 1: when encountering a NoWritesPerformed error after an error with a RetryableWriteError label
     */
    public function testRunForProseTest(): void
    {
        $runOn = [(object) ['topology' => [self::TOPOLOGY_REPLICASET]]];
        $this->checkServerRequirements($runOn);

        $client = self::createTestClient(null, ['retryWrites' => true]);

        // Step 2: Configure a fail point with error code 91
        $client->selectDatabase('admin')->command([
            'configureFailPoint' => 'failCommand',
            'mode' => ['times' => 1],
            'data' => [
                'writeConcernError' => [
                    'errorCode' => self::SHUTDOWN_IN_PROGRESS,
                    'errorLabels' => ['RetryableWriteError'],
                ],
                'failCommands' => ['insert'],
            ],
        ]);

        $subscriber = new class ($client) implements CommandSubscriber {
            private $client;

            public function __construct(Client $client)
            {
                $this->client = $client;
            }

            public function commandStarted(CommandStartedEvent $event): void
            {
            }

            public function commandSucceeded(CommandSucceededEvent $event): void
            {
                if ($event->getCommandName() === 'insert') {
                    // Step 3: Configure a fail point with code 10107
                    $this->client->selectDatabase('admin')->command([
                        'configureFailPoint' => 'failCommand',
                        'mode' => ['times' => 1],
                        'data' => [
                            'errorCode' => RetryableWritesSpecTest::NOT_PRIMARY,
                            'errorLabels' => ['RetryableWriteError', 'NoWritesPerformed'],
                            'failCommands' => ['insert'],
                        ],
                    ]);
                }
            }

            public function commandFailed(CommandFailedEvent $event): void
            {
            }
        };

        $client->getManager()->addSubscriber($subscriber);

        // Step 4: Run insertOne
        // Assert that error code is 91
        try {
            $client->selectCollection('db', 'retryable_writes')->insertOne(['write' => 1]);
        } catch (BulkWriteException $e) {
            $this->assertSame(self::SHUTDOWN_IN_PROGRESS, $e->getCode());
        }

        // Step 5: Disable the fail point
        $client->getManager()->removeSubscriber($subscriber);
        $client->selectDatabase('admin')->command([
            'configureFailPoint' => 'failCommand',
            'mode' => 'off'
        ]);
    }

    public static function provideNoWritableEventsTests(): Generator
    {
        // See: https://github.com/mongodb/specifications/commit/e4a5564a157cd877b09b52cc467988eb44818021#diff-974028a6b5a7e6c54bd76884a6212e54ecc1d5c325b7a2186d5847310a97657cR415
        yield 'Case 1: Configure a fail point with error code ``91`` (ShutdownInProgress)' => [
            static function (self $test, Client $client, CommandSubscriber $subscriber): void {

                try {
                    $client->selectCollection('db', 'retryable_writes')->insertOne(['write' => 1]);
                } catch (CommandException $e) {
                    $test->assertSame(self::SHUTDOWN_IN_PROGRESS, $e->getCode());
                }

                $test->assertNotNull($subscriber->lastAggregateReply);
            },
        ];

        // See: https://github.com/mongodb/specifications/commit/e4a5564a157cd877b09b52cc467988eb44818021#diff-974028a6b5a7e6c54bd76884a6212e54ecc1d5c325b7a2186d5847310a97657cR429
        yield 'Case 2: configure a fail point with error code ``10107`` (NotWritablePrimary) and a NoWritesPerformed label' => [
            static function (self $test, Client $client, CommandSubscriber $subscriber): void {

                try {
                    $client->selectCollection('db', 'retryable_writes')->insertOne(['write' => 1]);
                } catch (CommandException $e) {
                    $test->assertSame(self::NOT_PRIMARY, $e->getCode());
                }

                $test->assertNotNull($subscriber->lastAggregateError);
            },
        ];

        // See: https://github.com/mongodb/specifications/commit/e4a5564a157cd877b09b52cc467988eb44818021#diff-974028a6b5a7e6c54bd76884a6212e54ecc1d5c325b7a2186d5847310a97657cR450
        yield 'Case 3: Disable the fail point' => [
            static function (self $test, Client $client, CommandSubscriber $subscriber): void {
                $client->selectDatabase('admin')->command([
                    'configureFailPoint' => 'failCommand',
                    'mode' => 'off',
                ]);

                try {
                    $client->selectCollection('db', 'retryable_writes')->insertOne(['write' => 1]);
                } catch (CommandException $e) {
                    $test->assertSame(self::NOT_PRIMARY, $e->getCode());
                }

                $test->assertNotNull($subscriber->lastAggregateReply);
            },
        ];
    }
}
