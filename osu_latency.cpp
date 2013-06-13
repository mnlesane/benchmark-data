//  Copyright (c) 2013 Hartmut Kaiser
//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// Bidirectional network bandwidth test

#include <hpx/hpx_init.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/util/serialize_buffer.hpp>

#include <boost/assert.hpp>
#include <boost/shared_ptr.hpp>
#include <hpx/include/compression_snappy.hpp>
//#include <hpx/include/parcel_coalescing.hpp>

//HPX_ACTION_USES_MESSAGE_COALESCING(isend_action);
HPX_ACTION_USES_SNAPPY_COMPRESSION(isend_action);

///////////////////////////////////////////////////////////////////////////////
#define MESSAGE_ALIGNMENT 64
#define MAX_ALIGNMENT 65536
#define MAX_MSG_SIZE (1<<22)
#define SEND_BUFSIZE (MAX_MSG_SIZE + MAX_ALIGNMENT)

#define LOOP_LARGE  100
#define SKIP_LARGE  10

#define LARGE_MESSAGE_SIZE  8192


char send_buffer_orig[SEND_BUFSIZE];

///////////////////////////////////////////////////////////////////////////////
char* align_buffer (char* ptr, unsigned long align_size)
{
    return (char*)(((std::size_t)ptr + (align_size - 1)) / align_size * align_size);
}

#if defined(BOOST_MSVC)
unsigned long getpagesize()
{
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwPageSize;
}
#endif

///////////////////////////////////////////////////////////////////////////////
hpx::util::serialize_buffer<char>
message(hpx::util::serialize_buffer<char> const& receive_buffer)
{
    return receive_buffer;
}
HPX_PLAIN_ACTION(message);

///////////////////////////////////////////////////////////////////////////////
double receive(hpx::naming::id_type dest, char * send_buffer, std::size_t size, std::size_t window_size)
{
    int loop = LOOP_LARGE;
    int skip = SKIP_LARGE;

    typedef hpx::util::serialize_buffer<char> buffer_type;
    buffer_type recv_buffer;

    hpx::util::high_resolution_timer t;

    std::vector<hpx::future<buffer_type> > recv_buffers;
    recv_buffers.reserve(window_size);

    message_action msg;
    for (int i = 0; i != loop + skip; ++i) {
        // do not measure warm up phase
        if (i == skip)
            t.restart();

        for(std::size_t j = 0; j < window_size; ++j)
        {
            recv_buffers.push_back(hpx::async(msg, dest, buffer_type(send_buffer, size,
                buffer_type::reference)));
        }
        hpx::wait(recv_buffers);
    }

    double elapsed = t.elapsed();
    return (elapsed * 1e6) / (2 * loop * window_size);
}

///////////////////////////////////////////////////////////////////////////////
void print_header ()
{
    hpx::cout << "# OSU HPX Latency Test\n"
              << "# Size    Latency (microsec)\n"
              << hpx::flush;
}

///////////////////////////////////////////////////////////////////////////////
void run_benchmark(boost::program_options::variables_map & vm)
{
    // use the first remote locality to bounce messages, if possible
    hpx::id_type here = hpx::find_here();

    hpx::id_type there = here;
    std::vector<hpx::id_type> localities = hpx::find_remote_localities();
    if (!localities.empty())
        there = localities[0];
    
    // align used buffers on page boundaries
    unsigned long align_size = getpagesize();
    BOOST_ASSERT(align_size <= MAX_ALIGNMENT);
    char* send_buffer = align_buffer(send_buffer_orig, align_size);

    std::size_t window_size = vm["window-size"].as<std::size_t>();

    // perform actual measurements
    for (std::size_t size = 1; size <= MAX_MSG_SIZE; size *= 2)
    {
        double latency = receive(there, send_buffer, size, window_size);
        hpx::cout << std::left << std::setw(10) << size
                  << latency << hpx::endl << hpx::flush;
    }
}
