#ifndef BOOST_LOG_INIT_TAIRAN_HPP
#define BOOST_LOG_INIT_TAIRAN_HPP

#include <iostream>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/common.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/keyword.hpp>

#include <boost/log/attributes.hpp>
#include <boost/log/attributes/timer.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

#include <boost/log/attributes/named_scope.hpp>

#include <boost/log/trivial.hpp>

namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;

enum SeverityLevel
{
    Log_Info,
    Log_Notice,
    Log_Debug,
    Log_Warning,
    Log_Error,
    Log_Fatal
};

// The formatting logic for the severity level
    template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (
        std::basic_ostream< CharT, TraitsT >& strm, SeverityLevel lvl)
{
    static const char* const str[] =
    {
        "Info",
        "Notice",
        "Debug",
        "Warning",
        "Error",
        "Fatal"
    };
    if (static_cast< std::size_t >(lvl) < (sizeof(str) / sizeof(*str)))
        strm << str[lvl];
    else
        strm << static_cast< int >(lvl);
    return strm;
}

BOOST_LOG_ATTRIBUTE_KEYWORD(log_severity, "Severity", SeverityLevel)
BOOST_LOG_ATTRIBUTE_KEYWORD(log_timestamp, "TimeStamp", boost::posix_time::ptime)
BOOST_LOG_ATTRIBUTE_KEYWORD(log_uptime, "Uptime", attrs::timer::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(log_scope, "Scope", attrs::named_scope::value_type)


void g_InitLog(){
    logging::formatter formatter=
        expr::stream
        <<"["<<expr::format_date_time(log_timestamp,"%H:%M:%S")
        <<"]"
        << " <" << logging::trivial::severity
        << "> msg:"
        <<expr::message;

    logging::add_common_attributes();

    auto console_sink=logging::add_console_log();

    console_sink->set_formatter(formatter);
    /*
    auto file_sink=logging::add_file_log
        (
         keywords::file_name="%Y-%m-%d_%N.log",      //文件名
         keywords::rotation_size=10*1024*1024,       //单个文件限制大小
         keywords::time_based_rotation=sinks::file::rotation_at_time_point(0,0,0)    //每天重建
        );

    file_sink->locked_backend()->set_file_collector(sinks::file::make_collector(
                keywords::target="logs",        //文件夹名
                keywords::max_size=50*1024*1024,    //文件夹所占最大空间
                keywords::min_free_space=100*1024*1024  //磁盘最小预留空间
                ));


    file_sink->set_filter(log_severity>=Log_Warning);   //日志级别过滤
    file_sink->locked_backend()->scan_for_files();
    file_sink->set_formatter(formatter);
    file_sink->locked_backend()->auto_flush(true);
    logging::core::get()->add_sink(file_sink);
    */

    logging::core::get()->add_global_attribute("Scope",attrs::named_scope());
    logging::core::get()->add_sink(console_sink);
}

#endif
