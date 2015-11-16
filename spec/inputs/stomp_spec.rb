require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/stomp"

describe LogStash::Inputs::Stomp do
  let(:klass) { LogStash::Inputs::Stomp }
  let(:host) { "localhost" }
  let(:destination) { "/foo" }
  let(:stomp_settings) {
    {
      "host" => host,
      "destination" => destination
    }
  }
  let(:instance) { klass.new(stomp_settings) }

  context "when connected" do
    let(:client) { double("client") }

    before do
      allow(instance).to receive(:new_client).and_return(client)
      allow(client).to receive(:connect)
      allow(client).to receive(:connected?).and_return(true)
      allow(client).to receive(:on_connection_closed)
    end

    it "should register without error" do
      instance.register
    end
  end

  describe "stopping" do
    let(:config) { {"host" => "localhost", "destination" => "/foo"} }
    let(:client) { double("client") }
    before do
      allow(subject).to receive(:new_client).and_return(client)
      allow(subject).to receive(:connect)
      allow(client).to receive(:connect)
      allow(client).to receive(:connected?).and_return(true)
      allow(client).to receive(:on_connection_closed)
      allow(client).to receive(:subscribe)
    end
    it_behaves_like "an interruptible input plugin"
  end
end
