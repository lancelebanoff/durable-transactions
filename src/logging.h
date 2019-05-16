#ifndef LOGGING_H
#define LOGGING_H

#include <iostream>



class pmem_durableds_logger 
{

public:

enum log_severity_type
{
    debug = 1,
    error,
    none
};




void _pmem_durableds_log()
{
    std::cout << std::endl;	
};

template<typename First, typename...Rest >
void _pmem_durableds_log(First parm1, Rest...parm)
{
    std::cout <<parm1;
    _pmem_durableds_log(parm...);	
};

template<typename...Rest >
void pmem_durableds_log(log_severity_type s, Rest...parm)
{
    if(s >= _pmem_durableds_log_severity) {
        _pmem_durableds_log(parm...);
    }
};


pmem_durableds_logger(log_severity_type s);
// {
//     _pmem_durableds_log_severity = s;
// };


template<typename...Rest >
void pmem_durableds_dlog(Rest...parm)
{
    pmem_durableds_log(log_severity_type::debug, parm...);
};

template<typename...Rest >
void pmem_durableds_elog(Rest...parm)
{
    pmem_durableds_log(log_severity_type::error, parm...);
};

private:
log_severity_type _pmem_durableds_log_severity;


};


#endif