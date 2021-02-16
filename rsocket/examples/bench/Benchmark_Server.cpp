// Copyright (c) Facebook, Inc. and its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <thread>

#include <folly/init/Init.h>
#include <folly/portability/GFlags.h>

#include "rsocket/RSocket.h"
#include "rsocket/RSocketResponder.h"
#include "rsocket/transports/tcp/TcpConnectionAcceptor.h"
#include "rsocket/benchmarks/Latch.h"

using namespace rsocket;

DEFINE_int32(port, 9898, "port to connect to");

DEFINE_int32(threads, 1, "number of server threads to run");

DEFINE_int32(payload_size, 4096, "payload size in bytes");
DEFINE_int32(clients, 1, "number of clients to run");
DEFINE_int32(items, 1000, "number of items to fire-and-forget, per client");

class BenchmarkResponder : public rsocket::RSocketResponder {
public:
    BenchmarkResponder(Latch& latch, const std::string &message)
        : latch_{latch},
          message_{folly::IOBuf::copyBuffer(message)}
    {}

    std::shared_ptr<yarpl::single::Single<Payload>> handleRequestResponse(Payload, StreamId) override {
        return yarpl::single::Singles::fromGenerator<Payload>([this, msg = message_->clone()] {
           latch_.post();
           return Payload(msg->clone());
        });
    }

    std::shared_ptr<yarpl::flowable::Flowable<Payload>> handleRequestStream(Payload, StreamId) override {
        return yarpl::flowable::Flowable<Payload>::fromGenerator([this, msg = message_->clone()] {
           latch_.post();
           return Payload(msg->clone());
        });
    }

    void handleFireAndForget(Payload, StreamId) override {
        latch_.post();
    }

private:
    Latch& latch_;
    std::unique_ptr<folly::IOBuf> message_;
};

int main(int argc, char* argv[]) {
    FLAGS_logtostderr = true;
    FLAGS_minloglevel = 0;
    folly::init(&argc, &argv);

    Latch latch(static_cast<size_t>(FLAGS_items * FLAGS_clients));
    auto responder = std::make_shared<BenchmarkResponder>(latch, std::string(FLAGS_payload_size, 'a'));

    TcpConnectionAcceptor::Options opts;
    opts.address = folly::SocketAddress("::", FLAGS_port);
    opts.threads = FLAGS_threads;

    // RSocket server accepting on TCP
    auto rs = RSocket::createServer(std::make_unique<TcpConnectionAcceptor>(std::move(opts)));

    printf("Starting server...\n");
    rs->start([responder](const SetupParameters&) { return responder; });

    latch.wait();

    printf("Received %d items in %lu ms\n", FLAGS_items * FLAGS_clients, latch.elapsed_ms());

    return 0;
}
