package bunnyq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
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

type Option func(*config)

type config struct {
	logger  Logger
	threads int
	autoAck bool
	noLocal bool
	queues  map[string]queue
}

func Queue(name string, options ...QueueOption) func(cc *config) {
	q := queue{name: name}
	for _, option := range options {
		option(&q)
	}
	return func(cc *config) {
		if cc.queues == nil {
			cc.queues = make(map[string]queue)
		}
		cc.queues[q.name] = q
	}
}

func Threads(t int) func(cc *config) {
	return func(cc *config) {
		cc.threads = t
	}
}

func LogHandler(l Logger) func(cc *config) {
	return func(cc *config) {
		cc.logger = l
	}
}

func AutoAck(cc *config) {
	cc.autoAck = true
}

func NoLocal(cc *config) {
	cc.noLocal = true
}

type BunnyQ struct {
	logger      Logger
	address     Address
	connection  *amqp.Connection
	channel     *amqp.Channel
	queues      map[string]queue
	done        chan os.Signal
	notifyClose chan *amqp.Error
	isConnected bool
	alive       bool
	threads     int
	wg          *sync.WaitGroup
}

func Durable(q *queue) {
	q.durable = true
}

func DeleteUnused(q *queue) {
	q.deleteUnused = true
}

func Exclusive(q *queue) {
	q.exclusive = true
}

func NoWait(q *queue) {
	q.noWait = true
}

type QueueOption func(*queue)

type queue struct {
	name         string
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
func New(ctx context.Context, addr Address, done chan os.Signal, options ...Option) *BunnyQ {
	cc := &config{
		threads: 1,
	}
	for _, option := range options {
		option(cc)
	}
	client := BunnyQ{
		logger:  cc.logger,
		threads: cc.threads,
		done:    done,
		alive:   true,
		wg:      &sync.WaitGroup{},
		queues:  cc.queues,
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
		c.logger.Log(ctx, LogLevelInfo, "connecting to rabbitmq...", nil)
		var retryCount int
		for !c.connect(ctx, addr.string()) {
			if !c.alive {
				return
			}
			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				c.logger.Log(ctx, LogLevelWarn, "disconnected from rabbitmq and failed to connect", nil)
				retryCount++
			}
		}
		c.logger.Log(ctx, LogLevelInfo, "connected to rabbitmq",
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
	for name, q := range c.queues {
		_, err = ch.QueueDeclare(
			name,
			q.durable,      // Durable
			q.deleteUnused, // Delete when unused
			q.exclusive,    // Exclusive
			q.noWait,       // No-wait
			nil,            // Arguments
		)
	}

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
	autoAck       bool
	exclusive     bool
	noLocal       bool
	noWait        bool
	prefetchCount int
	prefetchSize  int
	global        bool
	consumerName  string
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

func StreamOpPrefetchCount(count int) func(s *streamOptions) {
	return func(s *streamOptions) {
		s.prefetchCount = count
	}
}

func StreamOpPrefetchSize(size int) func(s *streamOptions) {
	return func(s *streamOptions) {
		s.prefetchSize = size
	}
}

func StreamOpGlobal(s *streamOptions) {
	s.global = true
}

func StreamOpConsumer(name string) func(s *streamOptions) {
	return func(s *streamOptions) {
		s.consumerName = name
	}
}

func (c *BunnyQ) Stream(cancelCtx context.Context, queue string, handler func(delivery amqp.Delivery), options ...StreamOption) error {
	for {
		if c.isConnected {
			break
		}
		time.Sleep(1 * time.Second)
	}

	so := &streamOptions{
		prefetchCount: 1,
	}
	for _, option := range options {
		option(so)
	}

	err := c.channel.Qos(so.prefetchCount, so.prefetchSize, so.global)
	if err != nil {
		return err
	}

	var connectionDropped bool

	for i := 1; i <= c.threads; i++ {
		deliveryChannel, err := c.channel.Consume(
			queue,
			consumerName(so.consumerName, i), // BunnyQ
			so.autoAck,                       // Auto-Ack
			so.exclusive,                     // Exclusive
			so.noLocal,                       // No-local
			so.noWait,                        // No-Wait
			nil,                              // Args
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
	routingKey  string
	mandatory   bool
	immediate   bool
	contentType string
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

func PublishOpContentType(contentType string) func(p *publishOptions) {
	return func(p *publishOptions) {
		p.contentType = contentType
	}
}

func (c *BunnyQ) Publish(ctx context.Context, exchange string, body []byte, options ...PublishOption) error {
	err := c.channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

	po := &publishOptions{
		contentType: "application/json",
	}
	for _, option := range options {
		option(po)
	}

	err = c.channel.Publish(exchange,
		po.routingKey,
		po.mandatory,
		po.immediate,
		amqp.Publishing{
			ContentType: po.contentType,
			Body:        body,
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

func consumerName(name string, i int) string {
	if name == "" {
		name = "consumer"
	}
	return fmt.Sprintf("%s-%v", name, i)
}
