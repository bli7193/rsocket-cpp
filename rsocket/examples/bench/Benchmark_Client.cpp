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

#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GFlags.h>

#include "rsocket/RSocket.h"
#include "rsocket/examples/util/ExampleSubscriber.h"
#include "rsocket/transports/tcp/TcpConnectionFactory.h"

#include "yarpl/Single.h"
#include "rsocket/benchmarks/Latch.h"

using namespace rsocket;
using namespace std::chrono;

DEFINE_string(host, "localhost", "host to connect to");
DEFINE_int32(port, 9898, "host:port to connect to");

DEFINE_int32(payload_size, 4096, "payload size in bytes");
DEFINE_int32(threads, 1, "number of client threads to run");
DEFINE_int32(clients, 1, "number of clients to run");

DEFINE_int32(items, 1000, "number of items to send, per client");

DEFINE_string(mode, "fire-and-forget", "Modes are fire-and-forget, request-response, stream, channel");


static std::shared_ptr<RSocketClient> makeClient(folly::EventBase* eventBase,
                                                 folly::SocketAddress address) {
    auto factory = std::make_unique<TcpConnectionFactory>(*eventBase, std::move(address));
    return RSocket::createConnectedClient(std::move(factory)).get();
}

class Observer: public yarpl::single::SingleObserverBase<Payload> {
public:
    Observer(Latch& latch) : latch_{latch} {}

    void onSubscribe(std::shared_ptr<yarpl::single::SingleSubscription> sub) override {
        yarpl::single::SingleObserverBase<Payload>::onSubscribe(std::move(sub));
    }

    void onSuccess(Payload) override {
        latch_.post();
        yarpl::single::SingleObserverBase<Payload>::onSuccess({});
    }

    void onError(folly::exception_wrapper) override {
        printf("Hit error\n");
        latch_.post();
        yarpl::single::SingleObserverBase<Payload>::onError({});
    }

private:
    Latch &latch_;
};

class BoundedSubscriber : public yarpl::flowable::BaseSubscriber<Payload> {
public:
    BoundedSubscriber(Latch& latch, size_t requested)
        : latch_{latch}, requested_{requested} {}

    void onSubscribeImpl() override {
        this->request(requested_);
    }

    void onNextImpl(Payload) override {
        if (received_.fetch_add(1) == requested_ - 1) {
            terminated_.exchange(true);
            latch_.post(requested_);

            this->cancel();
        }
    }

    void onCompleteImpl() override {
        if (!terminated_.exchange(true)) {
            latch_.post(requested_);
        }
    }

    void onErrorImpl(folly::exception_wrapper) override {
        if (!terminated_.exchange(true)) {
            latch_.post(requested_);
        }
    }

private:
    Latch &latch_;
    size_t requested_;
    std::atomic<size_t> received_{0};
    std::atomic_bool terminated_{false};
};

enum Mode {
    FireAndForget,
    RequestResponse,
    Stream,
    Channel
};

static Mode parseMode(std::string str) {
    if (str == "fire-and-forget") {
        return FireAndForget;
    } else if (str == "request-response") {
        return RequestResponse;
    } else if (str == "stream") {
        return Stream;
    } else {
        printf("Invalid Mode\n");
        assert(0);
    }
}

int main(int argc, char* argv[]) {
    FLAGS_logtostderr = true;
    FLAGS_minloglevel = 0;
    folly::init(&argc, &argv);

    Mode mode = parseMode(FLAGS_mode);

    Latch latch(FLAGS_items * FLAGS_clients);

    std::vector<std::shared_ptr<RSocketClient>> clients;
    std::deque<std::unique_ptr<folly::ScopedEventBaseThread>> workers;

    folly::SocketAddress address;
    address.setFromHostPort(FLAGS_host, FLAGS_port);

    auto const numWorkers = FLAGS_threads;
    for (size_t i = 0; i < numWorkers; ++i) {
        workers.push_back(std::make_unique<folly::ScopedEventBaseThread>("rsocket-client-thread"));
    }

    for (size_t i = 0; i < FLAGS_clients; ++i) {
        auto worker = std::move(workers.front());
        workers.pop_front();
        clients.push_back(makeClient(worker->getEventBase(), address));
        workers.push_back(std::move(worker));
    }

    auto buf = std::string(FLAGS_payload_size, 'a');

    latch.start_time();

    for (int i = 0; i < FLAGS_items; ++i) {
        for (auto& client : clients) {
            switch (mode) {
            case FireAndForget:
                client->getRequester()
                    ->fireAndForget(Payload(buf))
                    ->subscribe(std::make_shared<yarpl::single::SingleObserverBase<void>>());
                latch.post();
                break;
            case RequestResponse:
                client->getRequester()
                    ->requestResponse(Payload(buf))
                    ->subscribe(std::make_shared<Observer>(latch));
                break;
            case Stream:
                client->getRequester()
                    ->requestStream(Payload("StartStream"))
                    ->subscribe(std::make_shared<BoundedSubscriber>(latch, FLAGS_items));
                break;
            default:
                assert(0);
                break;
            }
        }
    }

    latch.wait();

    printf("Processed %d items in %lu ms\n", FLAGS_items * FLAGS_clients, latch.elapsed_ms());

    if (latch.elapsed_ms() > 0) {
        auto total_bytes = (long long) (FLAGS_items * FLAGS_clients * FLAGS_payload_size);
        printf("KB/s: %llu\n", (total_bytes * 1000) / (latch.elapsed_ms()  * 1024));
    } else {
        printf("KB/s: N/A\n");
    }
    return 0;
}
