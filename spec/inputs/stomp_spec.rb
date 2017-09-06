require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/stomp"

describe LogStash::Inputs::Stomp do
  let(:host) { "localhost" }
  let(:destination) { "/foo" }
  let(:stomp_settings) {
    {
      "host" => host,
      "destination" => destination
    }
  }
  subject(:instance) { described_class.new(stomp_settings) }
  let(:client) { double("client") }

  before do
    allow(instance).to receive(:new_client).and_return(client)
    allow(client).to receive(:on_connection_closed)
    allow(instance).to receive(:new_client).and_return(client)
  end

  context "when registered" do
    it "should register without error" do
      instance.register
    end
  end

  context "connecting and disconnecting" do
    before do
      allow(client).to receive(:connect)
      allow(client).to receive(:connected?).and_return(true)
      allow(client).to receive(:subscribe)
      allow(client).to receive(:disconnect)
    end

    it "should connect and disconnect" do
      instance.register
      t = Thread.new { instance.run(Queue.new) }
      sleep 5
      expect(client).to have_received(:connect)
      instance.do_stop
      expect(client).to have_received(:disconnect)
      t.join
    end
  end
end
