package bunnyq

import (
	"context"
	"github.com/pkg/errors"
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"time"
)

type Logger interface {
	// Log a message at the given level with data key/value pairs. data may be nil.
	Log(ctx context.Context, level LogLevel, msg string, data map[string]interface{})
}

// LogLevel represents the pgx logging level. See LogLevel* constants for
// possible values.
type LogLevel int

// The values for log levels are chosen such that the zero value means that no
// log level was specified.
const (
	LogLevelTrace = 6
	LogLevelDebug = 5
	LogLevelInfo  = 4
	LogLevelWarn  = 3
	LogLevelError = 2
	LogLevelNone  = 1
)

var (
	ErrDisconnected = errors.New("disconnected from rabbitmq, trying to reconnect")
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second
)

type ConsumerOption func(*consumerConfig)

type consumerConfig struct {
	logger       Logger
	threads      int
	durable      bool
	deleteUnused bool
	exclusive    bool
	noWait       bool
	autoAck      bool
	noLocal      bool
}

func Threads(t int) func(cc *consumerConfig) {
	return func(cc *consumerConfig) {
		cc.threads = t
	}
}

func LogHandler(l Logger) func(cc *consumerConfig) {
	return func(cc *consumerConfig) {
		cc.logger = l
	}
}

func Durable(cc *consumerConfig) {
	cc.durable = true
}

func DeleteUnused(cc *consumerConfig) {
	cc.deleteUnused = true
}

func Exclusive(cc *consumerConfig) {
	cc.exclusive = true
}

func NoWait(cc *consumerConfig) {
	cc.noWait = true
}

func AutoAck(cc *consumerConfig) {
	cc.autoAck = true
}

func NoLocal(cc *consumerConfig) {
	cc.noLocal = true
}

type BunnyQ struct {
	logger       Logger
	address      Address
	connection   *amqp.Connection
	channel      *amqp.Channel
	queue        string
	done         chan os.Signal
	notifyClose  chan *amqp.Error
	isConnected  bool
	alive        bool
	threads      int
	wg           *sync.WaitGroup
	durable      bool
	deleteUnused bool
	exclusive    bool
	noWait       bool
}

type Address struct {
	User string
	Pass string
	Host string
	Port string
}

func (a *Address) string() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", a.User, a.Pass, a.Host, a.Port)
}

// New is a constructor that takes address, push and listen queue names, logger, and a channel that will notify rabbitmq
// client on server shutdown. We calculate the number of threads, create the client, and start the connection process.
// Connect method connects to the rabbitmq server and creates push/listen channels if they don't exist.
func New(ctx context.Context, queue string, addr Address, done chan os.Signal, options ...ConsumerOption) *BunnyQ {
	cc := &consumerConfig{
		threads: 1,
	}
	for _, option := range options {
		option(cc)
	}
	client := BunnyQ{
		logger:       cc.logger,
		threads:      cc.threads,
		done:         done,
		alive:        true,
		queue:        queue,
		wg:           &sync.WaitGroup{},
		durable:      cc.durable,
		deleteUnused: cc.deleteUnused,
		exclusive:    cc.exclusive,
		noWait:       cc.noWait,
	}
	client.wg.Add(cc.threads)

	go client.handleReconnect(ctx, addr)
	return &client
}

// handleReconnect will wait for a connection error on notifyClose, and then continuously attempt to reconnect.
func (c *BunnyQ) handleReconnect(ctx context.Context, addr Address) {
	for c.alive {
		c.isConnected = false
		t := time.Now()
		c.logger.Log(ctx, LogLevelInfo, "Attempting to connect to rabbitMQ", map[string]interface{}{"addr": addr})
		var retryCount int
		for !c.connect(ctx, addr.string()) {
			if !c.alive {
				return
			}
			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				c.logger.Log(ctx, LogLevelWarn, "disconnected from rabbitMQ and failed to connect", nil)
				retryCount++
			}
		}
		c.logger.Log(ctx, LogLevelInfo, "Connected to rabbitMQ",
			map[string]interface{}{"duration": time.Since(t).Milliseconds()})
		select {
		case <-c.done:
			return
		case <-c.notifyClose:
		}
	}
}

func (c *BunnyQ) logError(ctx context.Context, message string, err error) {
	c.logger.Log(ctx, LogLevelError, message, map[string]interface{}{"error": err})
}

// connect will make a single attempt to connect to RabbitMq. It returns the success of the attempt.
func (c *BunnyQ) connect(ctx context.Context, addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		c.logError(ctx, "failed to dial rabbitMQ server", err)
		return false
	}
	ch, err := conn.Channel()
	if err != nil {
		c.logError(ctx, "failed connecting to channel", err)
		return false
	}
	err = ch.Confirm(false)
	if err != nil {
		c.logger.Log(ctx, LogLevelWarn, "failed to set channel to confirm", nil)
	}
	_, err = ch.QueueDeclare(
		c.queue,
		c.durable,      // Durable
		c.deleteUnused, // Delete when unused
		c.exclusive,    // Exclusive
		c.noWait,       // No-wait
		nil,       // Arguments
	)
	if err != nil {
		c.logError(ctx, "failed to declare listen queue", err)
		return false
	}
	c.changeConnection(conn, ch)
	c.isConnected = true
	return true
}

// changeConnection takes a new connection to the queue, and updates the channel listeners to reflect this.
func (c *BunnyQ) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.notifyClose)
}

type StreamOption func(s *streamOptions)

type streamOptions struct {
	autoAck bool
	exclusive bool
	noLocal bool
	noWait bool
}

func StreamOpAutoAck(s *streamOptions) {
	s.autoAck = true
}

func StreamOpExclusive(s *streamOptions) {
	s.exclusive = true
}

func StreamOpNoLocal(s *streamOptions) {
	s.noLocal = true
}

func StreamOpNoWait(s *streamOptions) {
	s.noWait = true
}

func (c *BunnyQ) Stream(cancelCtx context.Context, handler func(delivery amqp.Delivery), options ...StreamOption) error {
	for {
		if c.isConnected {
			break
		}
		time.Sleep(1 * time.Second)
	}

	err := c.channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

	var connectionDropped bool

	so := &streamOptions{}
	for _, option := range options {
		option(so)
	}

	for i := 1; i <= c.threads; i++ {
		deliveryChannel, err := c.channel.Consume(
			c.queue,
			consumerName(i), // BunnyQ
			so.autoAck,       // Auto-Ack
			so.exclusive,     // Exclusive
			so.noLocal,       // No-local
			so.noWait,        // No-Wait
			nil,             // Args
		)
		if err != nil {
			return err
		}

		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-cancelCtx.Done():
					return
				case delivery, ok := <-deliveryChannel:
					if !ok {
						connectionDropped = true
						return
					}
					handler(delivery)
				}
			}
		}()
	}

	c.wg.Wait()

	if connectionDropped {
		return ErrDisconnected
	}

	return nil
}

type PublishOption func(p *publishOptions)

type publishOptions struct {
	routingKey string
	mandatory bool
	immediate bool
}

func PublishOpRoutingKey(key string) func(p *publishOptions) {
	return func(p *publishOptions) {
		p.routingKey = key
	}
}

func PublishOpMandatory(s *publishOptions) {
	s.mandatory = true
}

func PublishOpImmediate(s *publishOptions) {
	s.immediate = true
}

func (c *BunnyQ) Publish(ctx context.Context, exchange string, body string, options ...PublishOption) error {
	err := c.channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

	po := &publishOptions{}
	for _, option := range options {
		option(po)
	}

	err = c.channel.Publish(exchange,
		po.routingKey,
		po.mandatory,
		po.immediate,
		amqp.Publishing{
			ContentType: "application/json",
			Body: []byte(body),
		})
	if err != nil {
		return errors.WithMessage(err, "failed to publish to channel")
	}

	return nil
}

func (c *BunnyQ) Close() error {
	if !c.isConnected {
		return nil
	}
	c.alive = false
	fmt.Println("Waiting for current messages to be processed...")
	c.wg.Wait()
	for i := 1; i <= c.threads; i++ {
		fmt.Println("Closing consumer: ", i)
		err := c.channel.Cancel(consumerName(i), false)
		if err != nil {
			return fmt.Errorf("error canceling consumer %s: %v", consumerName(i), err)
		}
	}
	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.connection.Close()
	if err != nil {
		return err
	}
	c.isConnected = false
	fmt.Println("gracefully stopped rabbitMQ connection")
	return nil
}

func consumerName(i int) string {
	return fmt.Sprintf("consumer-%v", i)
}
