#include <cassert>
#include <chrono>
#include <iostream>
#include <string>

#ifndef _MADLEE_MISC_H_
#define _MADLEE_MISC_H_


namespace madlee {

    typedef std::string String;

    const long long SECONDS_IN_A_MINUTE = 60;
    const long long SECONDS_IN_AN_HOUR = SECONDS_IN_A_MINUTE*60;
    const long long SECONDS_IN_ONE_DAY = SECONDS_IN_AN_HOUR*24;

    extern double time_delta(double time_in_day);

    class Timer {
    public:
        Timer() { 
            reset();
        }

    public:
        void reset() {
            _start = std::chrono::system_clock::now();
        }

        double seconds() const {
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - _start
            );

            return double(duration.count() * std::chrono::microseconds::period::num) / std::chrono::microseconds::period::den;
        }

        friend std::ostream& operator<<(std::ostream& stream, const Timer& timer) {
            double s = timer.seconds();

            int days = int(s / (24*3600)); s -= days*24*3600;
            int hour = int(s / 3600);    s -= hour*3600;
            int min  = int(s / 60);      s -= min*60;
            char buffer[256];
            if (days) {
                sprintf(buffer, "%dD%02dH%02dM%.0fS", days, hour, min, s);
            }
            else if (hour) {
                sprintf(buffer, "%dH%02dM%.0fS", hour, min, s);
            }
            else if (min) {
                sprintf(buffer, "%dM%.3fS", min, s);
            }
            else if (s > 1) {
                sprintf(buffer, "%.3fS", s);
            }
            else if (s > 0.001) {
                sprintf(buffer, "%.3fmS", s*1000);
            }
            else {
                sprintf(buffer, "%.3fuS", s*1000000);
            }
            return stream << buffer;
        }

    private:
        std::chrono::time_point<std::chrono::system_clock> _start;
    };


    // A Time expressed in double HHMMSS.mmm
    class TimeInExpress {
        
    public:
        explicit TimeInExpress(double t) 
            : _t(t)
        { }

        double time() const {
            return _t;
        }

        int hour() const {
            return int(_t/10000);
        }

        int minute() const {
            return int(_t) / 100 % 100;
        }

        double second() const {
            return _t - 10000*hour() - 100*minute();
        }

        TimeInExpress& operator+=(double seconds) {
            if (seconds > 0) {
                _t = __increase(seconds);
            }
            else {
                _t = __decrease(-seconds);
            }
            return *this;
        }

        TimeInExpress operator+(double seconds) {
            TimeInExpress result(*this);
            result += seconds;
            return result;
        }

        TimeInExpress& operator-=(double seconds) {
            if (seconds < 0) {
                _t = __increase(-seconds);
            }
            else {
                _t = __decrease(seconds);
            }
            return *this;
        }

        TimeInExpress operator-(double seconds) {
            TimeInExpress result(*this);
            result -= seconds;
            return result;
        }
        
    private:
        double _t;

    private:
        double __increase(double seconds) const {
            int h = hour(), m = minute(), s = second();
            s += seconds;
            if (s >= 60) {
                int dm = int(s / 60);
                m += dm;
                s -= dm*60;
            }
            if (m >= 60) {
                h += m / 60;
                m %= 60;
            }
            return (h*100 + m)*100 + s;
        }
        double __decrease(double seconds) const {
            int h = hour(), m = minute(), s = second();
            s -= seconds;
            if (s < 0) {
                int dm = int(s / 60);
                s -= dm*60;
                if (s < 0) {
                    assert (s > -60);
                    s += 60;
                    dm -= 1;
                }
                m -= dm;
            }
            if (m < 0) {
                int dh = m / 60;
                m %= 60;
                if (m < 0) {
                    assert (m > -60);
                    m += 60;
                    dh -= 1;
                }
                h -= dh;
            }
            return (h*100 + m)*100 + s;
        }
    };

} 

#endif // _MADLEE_MISC_H_


