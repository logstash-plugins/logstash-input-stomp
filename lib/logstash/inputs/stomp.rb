# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "pp"

# Creates events received with the STOMP protocol.
class LogStash::Inputs::Stomp < LogStash::Inputs::Base
  attr_accessor :client

  config_name "stomp"

  default :codec, "plain"

  # The address of the STOMP server.
  config :host, :validate => :string, :default => "localhost", :required => true

  # The port to connet to on your STOMP server.
  config :port, :validate => :number, :default => 61613

  # The username to authenticate with.
  config :user, :validate => :string, :default => ""

  # The password to authenticate with.
  config :password, :validate => :password, :default => ""

  # The destination to read events from.
  #
  # Example: `/topic/logstash`
  config :destination, :validate => :string, :required => true

  # The vhost to use
  config :vhost, :validate => :string, :default => nil

  # Auto reconnect
  config :reconnect, :validate => :boolean, :default => true
  
  #Auto reconnect interval in seconds
  config :reconnect_interval, :validate => :number, :default => 30

  # Enable TLS/SSL connection?
  config :ssl, :validate => :boolean, :default => false

  # Validate TLS/SSL certificate?
  config :ssl_certificate_validation, :validate => :boolean, :default => true

  # If you need to use a custom X.509 CA (.pem certs) specify the path to that here
  config :cacert, :validate => :path

  # If you'd like to use a client certificate (note, most people don't want this) set the path to the x509 cert here
  config :client_cert, :validate => :path

  # If you're using a client certificate specify the path to the encryption key here
   config :client_key, :validate => :path

  # Enable debugging output?
  config :debug, :validate => :boolean, :default => false

  private
  def connect
    begin
      @client.connect
      @logger.info("Connected to stomp server") if @client.connected?
    rescue OnStomp::ConnectFailedError, OnStomp::UnsupportedProtocolVersionError, Errno::ECONNREFUSED => e      
      if @reconnect && !stop?
        @logger.warn("Failed to connect to stomp server. Retry in #{@reconnect_interval} seconds. #{e.inspect}")
        @logger.debug("#{e.backtrace.join("\n")}") if @debug
        sleep @reconnect_interval
        retry
      end

      @logger.warn("Failed to connect to stomp server. Exiting with error: #{e.inspect}")
      @logger.debug("#{e.backtrace.join("\n")}") if @debug
    end
  end

  public
  def register
    require "onstomp"
    # Set protocol based on wether ssl is true or not
    if @ssl
      @protocol = "stomp+ssl"
      @ssl_opts = {}
      @ssl_opts[:ca_file] = @cacert if @cacert
      @ssl_opts[:cert] = @client_cert if @client_cert
      @ssl_opts[:key] = @client_key if @client_key
      # disable verification if false
      if !@ssl_certificate_validation
        @ssl_opts[:verify_mode] = OpenSSL::SSL::VERIFY_NONE
        @ssl_opts[:post_connection_check] = false
      end
    else
      @protocol = "stomp"
      @ssl_opts = {}  # no ssl options if not ssl
    end
    @client = new_client
    @client.host = @vhost if @vhost
    @stomp_url = "#{@protocol}://#{@user}:#{@password}@#{@host}:#{@port}/#{@destination}"
  end # def register

  def new_client
    OnStomp::Client.new("#{@protocol}://#{@host}:#{@port}", :login => @user, :passcode => @password.value, :ssl => @ssl_opts)
  end

  private
  def subscription_handler
    #Exit function when connection is not active
    return if !@client.connected?

    @client.subscribe(@destination) do |msg|
      @codec.decode(msg.body) do |event|
        decorate(event)
        @output_queue << event
      end
    end
    #In the event that there is only Stomp input plugin instances
    #the process ends prematurely. The above code runs, and return
    #the flow control to the 'run' method below. After that, the
    #method "run_input" from agent.rb marks 'done' as 'true' and calls
    #'finish' over the Stomp plugin instance.
    #'Sleeping' the plugin leaves the instance alive.
    until stop?
      sleep 1
    end
  end

  public
  def run(output_queue)
    # Handle disconnects
    @client.on_connection_closed {
      self.connect
      subscription_handler # is required for re-subscribing to the destination
    }

    connect
    @output_queue = output_queue
    subscription_handler
  end # def run

  def stop
    @client.disconnect if @client && @client.connected?
  end
end # class LogStash::Inputs::Stomp
