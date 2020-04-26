#include <chrono>
#include "misc.h"

namespace madlee {

    extern double time_delta(double time_in_day) {
        
        long hour = long(time_in_day) / 10000;
        long minute = long(time_in_day) % 10000 / 100;

        time_in_day -= hour*10000+minute*100;
        time_in_day += SECONDS_IN_AN_HOUR*(hour-8)+SECONDS_IN_A_MINUTE*minute;

        auto now = std::chrono::high_resolution_clock::now();
        auto time = now.time_since_epoch();
        double now_in_day = time.count() % (SECONDS_IN_ONE_DAY*decltype(time)::period::den);
        now_in_day /= decltype(time)::period::den;

        return now_in_day-time_in_day;
    }


} }

