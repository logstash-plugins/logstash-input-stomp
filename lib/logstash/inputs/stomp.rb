# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require 'pp'


class LogStash::Inputs::Stomp < LogStash::Inputs::Base
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

  # Enable TLS/SSL connection?
  config :ssl, :validate => :boolean, :default => false

  # Validate TLS/SSL certificate?
  config :verify_ssl, :validate => :boolean, :default => false

  # Enable debugging output?
  config :debug, :validate => :boolean, :default => false
  
  private
  def connect
    begin
      @client.connect
      @logger.debug("Connected to stomp server") if @client.connected?
    rescue => e
      @logger.debug("Failed to connect to stomp server, will retry", :exception => e, :backtrace => e.backtrace)
      sleep 2
      retry
    end
  end

  public
  def register
    require "onstomp"
    # Set protocol based on wether ssl is true or not
    if @ssl
      @protocol = "stomp+ssl"
      if @verify_ssl
        @ssl_opts = {}  # default in onstomp is to verify certificates
      else
        @ssl_opts = {:verify_mode => OpenSSL::SSL::VERIFY_NONE, :post_connection_check => false}  # disable verification
      end
      @client = OnStomp::Client.new("stomp+ssl://#{@host}:#{@port}", :login => @user, :passcode => @password.value, :ssl => @ssl_opts)
    else
      @protocol = "stomp"
      @client = OnStomp::Client.new("stomp://#{@host}:#{@port}", :login => @user, :passcode => @password.value)
    end
    @client.host = @vhost if @vhost
    @stomp_url = "#{@protocol}://#{@user}:#{@password}@#{@host}:#{@port}/#{@destination}"

    # Handle disconnects 
    @client.on_connection_closed {
      connect
      subscription_handler # is required for re-subscribing to the destination
    }
    connect
  end # def register

  private
  def subscription_handler
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
    #'Sleeping' the plugin leves the instance alive.
    sleep
  end

  public
  def run(output_queue)
    @output_queue = output_queue 
    subscription_handler
  end # def run
end # class LogStash::Inputs::Stomp
