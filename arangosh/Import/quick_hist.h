////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGODB_IMPORT_QUICK_HIST_H
#define ARANGODB_IMPORT_QUICK_HIST_H 1

#include <chrono>
#include <mutex>
#include <vector>

namespace arangodb {
namespace import {
//
// quickly written histogram class for debugging.  Too awkward for
//  production
//
class QuickHistogram {
 private:
  QuickHistogram(QuickHistogram const&) = delete;
  QuickHistogram& operator=(QuickHistogram const&) = delete;

 public:
  QuickHistogram()
  {
    _interval_start=std::chrono::steady_clock::now();
    _measuring_start = _interval_start;
    _sum = std::chrono::microseconds(0);
  }


  ~QuickHistogram()
  {
    print_interval(true);
  }


  void post_latency(std::chrono::microseconds latency) {
    {
      std::lock_guard<std::mutex> lg(_mutex);

      _latencies.push_back(latency);
      _sum += latency;
    }

    print_interval();
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief
  //////////////////////////////////////////////////////////////////////////////
  std::chrono::steady_clock::time_point _measuring_start;
  std::chrono::steady_clock::time_point _interval_start;

  std::vector<std::chrono::microseconds> _latencies;
  std::chrono::microseconds _sum;

  std::mutex _mutex;

 protected:
  void print_interval(bool force=false) {
    std::chrono::steady_clock::time_point interval_end;
    std::chrono::milliseconds interval_diff, measuring_diff;

    interval_end=std::chrono::steady_clock::now();
    interval_diff=std::chrono::duration_cast<std::chrono::milliseconds>(interval_end - _interval_start);

    if (std::chrono::milliseconds(10000) <= interval_diff || force) {
      std::lock_guard<std::mutex> lg(_mutex);

      // retest within mutex
      if (std::chrono::milliseconds(10000) <= interval_diff || force) {
        double fp_measuring, fp_interval;
        size_t num=_latencies.size();

        measuring_diff = std::chrono::duration_cast<std::chrono::milliseconds>(interval_end - _measuring_start);
        fp_measuring = measuring_diff.count() / 1000.0;
        fp_interval = interval_diff.count() / 1000.0;

        if (0==num) {
          _latencies.push_back(std::chrono::microseconds(0));
          num=1;
        }

        std::chrono::microseconds mean, median, per95, per99, per99_9;
        mean = _sum / num;

        sort(_latencies.begin(), _latencies.end());
        bool odd(num & 1);
        size_t half(num/2), int95, int99, int99_9;

        int95 = (num * 95) / 100;
        int99 = (num * 99) / 100;
        int99_9 = (num * 999) / 1000;

        if (1==num) {
          median = _latencies[0];
        } else if (odd) {
          median = (_latencies[half] + _latencies[half +1]) / 2;
        } else {
          median = _latencies[half];
        }

        // close but not exact math for percentiles
        per95 = _latencies[int95];
        per99 = _latencies[int99];
        per99_9 = _latencies[int99_9];

        printf("%.3f,%.3f,%zd,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%d,%d\n",
               fp_measuring, fp_interval, num, _latencies[0].count(),
               mean.count(),median.count(),
               per95.count(), per99.count(), per99_9.count(), _latencies[num-1].count(), 0, 0);
        _latencies.clear();
        _interval_start=interval_end;
        _sum=std::chrono::microseconds(0);
      }
    }
  }

 private:
};


class QuickHistogramTimer {
public:
  QuickHistogramTimer(QuickHistogram & histo)
    : _histogram(histo) {
    _interval_start=std::chrono::high_resolution_clock::now();
  }

  ~QuickHistogramTimer() {
    std::chrono::microseconds latency;

    latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - _interval_start);
    _histogram.post_latency(latency);
  }

  std::chrono::high_resolution_clock::time_point _interval_start;
  QuickHistogram & _histogram;
}; // QuickHistogramTimer
}
}
#endif
