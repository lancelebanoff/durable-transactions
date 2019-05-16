#include <iostream>
#include "logging.h"

pmem_durableds_logger::pmem_durableds_logger(log_severity_type s)
{
    _pmem_durableds_log_severity = s;
}