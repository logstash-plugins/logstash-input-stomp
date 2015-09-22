require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/stomp"

describe LogStash::Inputs::Stomp do
  describe "stopping" do
    let(:config) { {"host" => "localhost", "destination" => "/foo"} }
    let(:client) { double("client") }
    before do
      subject.client = client
      allow(subject).to receive(:connect)
      allow(client).to receive(:subscribe)
    end
    it_behaves_like "an interruptible input plugin"
  end
end