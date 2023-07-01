# Message bus

## Description
This library is intended for supporting of outbox pattern in general. And it can be used for publishing/subscribing of 
domain events. But of course it can be used as a queue on your db.

### The library supports:
* publishing messages (single and batch)
* consuming with multiple subscriptions (single and batch)
* retrying of failed messages
* blocking subscription processing in case of unavailable resources 
* automatic creation of tables and daily partitions for messages and subscriptions
* automatic clearing partitions

## Usage
### Initialization
First of all you need to initialize the message bus, it should be one instance per application. It can be a singleton bean in Spring.
```java
    //init message bus
    PersistenceService persistenceService = new SpringPersistenceService(jdbcTemplate);
    QueryService queryService = new PgQueryService(persistenceService, "schema", jsonbConverter);
    TransactionService transactionService = new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource) {}));
    CronConfig cronConfig = CronConfig.builder().build()
    MessageBus messageBus = new MessageBus(
            queryService,
            transactionService, 
            new StdMessageFactory(),
            cronConfig);
```
### Publishing 
#### Registration
To start publishing messages you need to register publishers, it consumes publisher config with message name and class. 
Message name is a unique identifier of message type. It is used for example during creation of tables. 
Besides it 
```java
    //register publisher 
    messageBus.registerPublisher(PublisherConfig.<Message>builder()
        .messageName(MESSAGE_NAME)
        .clazz(Message.class)
        .build());
```
#### Sending
Message class should be serializable to JSON, because it will be serialized and stored in db.
To publish message you need to call publish method with message name and message.
```java
    //publish message
    messageBus.publish(MESSAGE_NAME, new Message("test"));
```
### Subscribing
#### Registration
To start consuming messages you need to register subscribers, it consumes subscriber config with message name and subscription name. 
Subscription name is a unique identifier of subscriber. Besides you can specify properties for subscriber, for example concurrency. 
The most important property is listener where should be placed your business logic.
```java
    //register subscriber
    messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
        .messageListener(message -> { // your business logic
            System.out.println("Message received: " + message);
        })
        .messageName(MESSAGE_NAME) // unique identifier of message type
        .subscriptionName(SUBSCRIPTION_NAME) // unique identifier of subscription group
        .properties(SubscriberConfig.Properties.builder()
            .concurrency(10) // number of threads for consuming messages
            .build())
        .build());
```
#### Starting
To start consuming messages you need to call startSubscribers method.
```java
    //start subscribers
    messageBus.startSubscribers();
```
#### Stopping
To stop consuming messages you need to call stopSubscribers method.
```java
    //stop subscribers
    messageBus.stopSubscribers();
```

## Abstraction layers:
The library support abstraction layers for:
* database queries layer - it can be used with any database that supports SQL that support select for update skip locked,
  default impl is for PostgreSQL
* persistence layer - it can be used with any frameworks that supports jdbc operations, default impl is for Spring

## Outbox 
### Publishing
You should always publish your messages(events) in the same transaction with your business logic. Only in this case you can be 
sure that your message will be published correctly.
### Subscribing
Messages are consumed and processed in the same transaction. It gives you guarantee that your business logic changes will be 
committed with correct message subscription status(PROCESSED or FAILED). Be careful with maxPollRecords property, transaction is started 
on all messages that are polled from db. And if you have message listener with high running time you can face with long 
transactions. Please don't change maxPollRecords, it is equaled 1 by default, if you don't have high load on your system.

## Retry
During processing of message you can throw exception, in this case message subscription will be marked as FAILED or retried. 
You can configure retryable policy, blocking policy, and non retrayble policy for each subscriber. Please see SubscriberConfig.
### Retryable policy
It consumes Exception and attempt and returns duration. Please see StdRetryablePolicy for details. All your exceptions 
by default is retryable. If you want to mark some exceptions as non retryable you should use NonRetryablePolicy.
### Non retryable policy
It consumes Exception and returns boolean. If it returns true message will be marked as FAILED at once.
### Blocking policy
It consumes attempt and returns duration. You can use it for blocking message processing in case of unavailable your 
resources. In this case message will be retried after duration and attempt will not be incremented.