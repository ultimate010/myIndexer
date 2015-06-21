#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <set>
#include <leveldb/db.h>
// #include "boost_logger.hpp"
#include "utf8.h"
#include "logger.hpp"
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "boost/format.hpp"

#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#include <ctime>



namespace fs = boost::filesystem;
using namespace std;

// Global variable
leveldb::DB *g_db;
bool g_rebuild = true;
long DATA_MAX_SIZE = (1024 * 1);
long SLICE_WRITE_SIZE = (1024 * 1024 * 10);
string g_data_path;
string g_index_path;
string g_db_path;
auto g_ch_size = sizeof (unsigned short);
auto g_offset = g_ch_size * 4;
vector<string> g_docs;
map<int, string> g_id2file_map;
map<string, int> g_file2id_map;
map<string, pair<int,long> > g_file2dataid_pos_map;
map<int, pair<int, long > > g_doc2startpos_map;
map<string, int> g_char2slice_map;




void u16_2_u8(const vector<unsigned short> & utf16line, string & line_out);
string ch_2_u8(unsigned short ch);
void clean_line(string & line);
template<class type> void compose_data_name(const type & outfile_index, string & outFileName);

void clean_line(string & line) {
    if (!line.empty() && line[line.size() - 1] == '\r') {
        line.erase(line.size() - 1);
    }
}

bool create_dir(const char * index_path) {
    if (g_rebuild) {
        fs::remove_all(index_path);
    }
    if (!fs::exists(index_path)) {
        fs::create_directory(index_path);
        LogInfo("Creating: %s dir", index_path);
    } else {
        LogInfo("Use %s dir" , index_path);
    }
    return true;
}

template<class type> void compose_data_name(const type & outfile_index, string & outFileName) {
    fs::path dir(g_index_path);
    outFileName = str( boost::format("DATA_%#05x") % outfile_index );
    outFileName = (dir / fs::path(outFileName)).string();
}

template<class type1, class type2> void compose_two_key(const type1 & key1, const type2 & key2, string & keyName) {
    keyName = str( boost::format("%d_%#05x_%#05x") % key1 % key1 % key2); // From 0 to N
}
template<class type1, class type2> void compose_slice_key(const type1 & key1, const type2 & key2, string & keyName) {
    keyName = str( boost::format("%s_%#05x") % key1 % key2); // From 0 to N
}
void find_section(vector < pair < int, long > > & A,
        vector < pair < int, long > > & B,
        vector < pair < int, long > > & C,
        int pos=1) {
    auto i = 0l;
    auto j = 0l;
    C.clear();
    for ( ; i < A.size(); i++) {
        pair<int, long> a_pos = A[i];
        for ( ; j < B.size(); j++) {
            pair<int, long> b_pos = B[j];
            if (a_pos.first < b_pos.first) {
                break;  // Wait for i
            } else if (a_pos.first == b_pos.first) {
                if ( ( b_pos.second - a_pos.second ) <= pos && ( b_pos.second > a_pos.second ) ) {
                    C.push_back(make_pair(b_pos.first, b_pos.second));  // Add the pos
                }
                if (b_pos.second > a_pos.second) {
                    break;
                } else {
                    continue;
                }
            } else {
                continue;  // Wait for j
            }
        }
        if ( j == B.size() ) {
            return ;
        }
    }
}

struct recursive_directory_range {
    typedef fs::recursive_directory_iterator iterator;
    recursive_directory_range(fs::path p) : p_(p) {}

    iterator begin() {
        return fs::recursive_directory_iterator(p_);
    }
    iterator end() {
        return fs::recursive_directory_iterator();
    }

    fs::path p_;
};

bool init_db(const string & dbpath, leveldb::DB ** db) {
    if (g_rebuild) {
        create_dir(dbpath.c_str());
    }
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open( options, dbpath, db );
    assert ( status.ok() );
    return true;
}

bool save_db(const string & key, const string & value) {
    // clock_t tStart = clock();
    leveldb::Status sta;
    sta = g_db->Put( leveldb::WriteOptions(), key, value);
    // printf("Time taken: %.9fs\n", (double)(clock() - tStart)/CLOCKS_PER_SEC);
    if ( sta.ok() ) {
        return true;
    }
    return false;
}

bool get_db(const string & key, string & value) {

    //clock_t tStart = clock();
    leveldb::Status sta;
    sta = g_db->Get( leveldb::ReadOptions(), key, &value );
    //printf("Time taken: %.9fs\n", (double)(clock() - tStart)/CLOCKS_PER_SEC);
    if ( sta.ok() ) {
        return true;
    }
    return false;
}


bool get_all_file(const char * path, vector<string> & vs) {
    try {
        for (auto it : recursive_directory_range(path)) {
            vs.push_back(it.path().string());
        }
        return true;
    } catch (exception& e) {
        LogFatal(e.what());
        return false;
    }
}

void write_string_record(ofstream & of, const string & filename) {
    long c = filename.size();
    of.write(reinterpret_cast<char *>(&c),  sizeof(c));
    of.write(reinterpret_cast<const char *>(filename.c_str()), c);
}

void read_string_record(ifstream & ifs, stringstream & istr) {
    long count = 0;
    ifs.read(reinterpret_cast<char *>(&count), sizeof(count));
    unique_ptr<char []> tmp(new char[count]);
    ifs.read(reinterpret_cast<char *>(tmp.get()), count);
    istr.write(tmp.get(), count);
}

template <class type1 > void str_2_obj(stringstream & in, type1 & out) {
    boost::archive::binary_iarchive ia(in);
    ia >> out;
}

template <class type1, class type2> void set_or_plus(type1 & m, const type2 & k) {
    if (m.find(k) != m.end()) {
        m[k] = m[k] + 1;
    } else {
        m[k] = 0;
    }
}


template <class type1, class type2, class type3> void set_or_pushback(type1 & m,
        const type2 & k,
        const type3 & v) {
    if (m.find(k) != m.end()) {
        m[k].push_back(v);
    } else {
        vector< pair<int, long> > vc;
        m[k] = vc;
        m[k].push_back(v);
    }
}


template <class type1> void obj_2_str(const type1 & in, string & out) {
    stringstream ostr;
    boost::archive::binary_oarchive oa(ostr);
    oa << in;
    out = ostr.str();
}


template <class type1, class type2, class type3, class type4> void write_slice_record(type1 & char2count_map,
        type2 & char2slice_map,
        type3 & char2pos_map,
        type4 & key) {
    try {
        char2count_map[key] = 0;
        vector < pair<int, long> > & ref = char2pos_map[key];
        string out;
        obj_2_str<vector <pair <int, long > > > (ref, out);
        set_or_plus(char2slice_map, key);
        string keyName;  // From 0 to N
        compose_slice_key(key, char2slice_map[key], keyName);
        save_db(keyName, out);  // Save the vector to db
        ref.clear();
    } catch (exception& e) {
        LogFatal(e.what());
    }
}

string ch_2_u8(unsigned short ch) {
    vector<unsigned short> vs;
    vs.push_back(ch);
    string s;
    u16_2_u8(vs, s);
    return s;
}

template <class type1, class type2> void clear_write(type1 & char2pos_map,
        type2 & char2count_map,
        type2 & char2slice_map
        ) {
    for (auto it: char2pos_map) {
        write_slice_record(char2count_map, char2slice_map, char2pos_map, it.first);
    }
}

void u8_2_u16(string & line, vector<unsigned short> & utf16line) {
    string::iterator end_it = utf8::find_invalid(line.begin(), line.end());
    utf8::utf8to16(line.begin(), end_it, back_inserter(utf16line));
}

void u16_2_u8(const vector<unsigned short> & utf16line, string & line_out) {
    utf8::utf16to8(utf16line.begin(), utf16line.end(), back_inserter(line_out));
}


bool build_index(const char * index_path) {
    try {

        vector<string> vs;
        get_all_file(g_data_path.c_str(), vs);

        create_dir(index_path);
        fs::path dir(index_path);
        fs::path file("_index");
        fs::path full_path = dir / file;
        ofstream fidx(full_path.string(), ios::binary);
        if (!fidx) {
            LogFatal("Can not open %s", full_path.string().c_str());
        }
        map<int, string> id2file_map;
        map<string, int> file2id_map;
        map<string, pair<int,long> > file2dataid_pos_map;
        map<int, pair<int, long > > doc2startpos_map;  // The pos which doc data store in data file  <data_file_id, pos>

        map<string, vector< pair<int, long> > > char2pos_map; // char_char to <doc_id, doc_offset>
        map<string, int> char2count_map;  // Current char_char times count, use for write db count
        map<string, int> char2slice_map;  // Slice count from like A_A_0 A_A_1 A_B_0 

        int count = 0;
        for (auto it: vs) {
            file2id_map[it] = count;
            id2file_map[count] = it;
            count++;
        }

        long outfile_index = 0;
        long outfile_count = DATA_MAX_SIZE + 10000;  // record current fileCount
        long writer_pos = 0;

        ofstream outfile;

        string out;
        obj_2_str<vector<string> > (vs, out);
        write_string_record(fidx, out);

        for (auto filename: vs) {
            LogInfo("Indexing : %s" , filename.c_str());

            if (outfile_index == 0) {
                doc2startpos_map[file2id_map[filename]] = make_pair(outfile_index, writer_pos);
            } else {
                doc2startpos_map[file2id_map[filename]] = make_pair(outfile_index - 1, writer_pos);
            }

            ifstream infile(filename);
            long char_offset = 0;

            string line;
            auto linecount = 0;
            while (std::getline(infile, line)) {
                clean_line(line);
                if (++linecount % 10000 == 0) {
                    LogInfo("Line: %d" , linecount);
                }

                vector<unsigned short> utf16line;
                u8_2_u16(line, utf16line);
                unsigned short pre_ch = 0xFF;

                for (auto u16char: utf16line) {
                    string new_key;
                    
                    compose_two_key(pre_ch, u16char, new_key);
                    {
                        set_or_plus(char2count_map, new_key);
                        set_or_pushback(char2pos_map, new_key, make_pair(file2id_map[filename], char_offset) );
                        if (char2count_map[new_key] >= SLICE_WRITE_SIZE ) {
                            write_slice_record(char2count_map, char2slice_map, char2pos_map, new_key);
                        }
                    }
                    if (pre_ch != 0xFF)
                    {
                        compose_two_key(0xFF, u16char, new_key);
                        set_or_plus(char2count_map, new_key);
                        set_or_pushback(char2pos_map, new_key, make_pair(file2id_map[filename], char_offset) );
                        if (char2count_map[new_key] >= SLICE_WRITE_SIZE ) {
                            write_slice_record(char2count_map, char2slice_map, char2pos_map, new_key);
                        }
                    }
                    if (outfile_count >= DATA_MAX_SIZE) {
                        // Create new file
                        outfile_count = 0;
                        writer_pos = 0;
                        string outFileName;
                        compose_data_name<int>(outfile_index, outFileName);
                        outfile.close();
                        outfile.open(outFileName);
                        outfile_index++;
                    }

                    outfile.write(reinterpret_cast<char *>(&u16char),  g_ch_size);  // write char to file
                    outfile_count += g_ch_size;
                    writer_pos += g_ch_size;
                    char_offset++;
                    pre_ch = u16char;
                }
            }
        }

        // write_info2indexfile


        obj_2_str< map<int, string> > (id2file_map, out);
        write_string_record(fidx, out);
        obj_2_str< map<string, int> > (file2id_map, out);
        write_string_record(fidx, out);
        obj_2_str< map<string, pair<int,long> > > (file2dataid_pos_map, out);
        write_string_record(fidx, out);
        obj_2_str< map<int, pair<int, long > >  > (doc2startpos_map, out);
        write_string_record(fidx, out);

        clear_write(char2pos_map, char2count_map, char2slice_map);  // Write the last bucket to the db

        obj_2_str< map<string, int>  > (char2slice_map, out);
        write_string_record(fidx, out);

        return true;
    } catch (exception& e) {
        LogFatal(e.what());
        return false;
    }
}

template <class type1> void read_obj(ifstream & fin, type1 & obj) {
    stringstream sbuf;
    read_string_record(fin, sbuf);
    str_2_obj(sbuf, obj);
}


bool read_index(const char * index_path) {
    try {
        fs::path dir(index_path);
        fs::path file("_index");
        fs::path full_path = dir / file;
        ifstream fidx(full_path.string(), ios::binary);
        if (!fidx) {
            LogFatal("Can not open %s" , full_path.string().c_str());
        }
        LogInfo("Reading index %s ", full_path.string().c_str());
        read_obj(fidx, g_docs);
        read_obj(fidx, g_id2file_map);
        read_obj(fidx, g_file2id_map);
        read_obj(fidx, g_file2dataid_pos_map);
        read_obj(fidx, g_doc2startpos_map);
        read_obj(fidx, g_char2slice_map);
        return true;
    } catch (exception& e) {
        LogFatal(e.what());
        return false;
    }
}

void load_config(const char * path) {
    boost::property_tree::ptree pt;
    boost::property_tree::ini_parser::read_ini(path, pt);
    g_data_path = pt.get<string>("all.data_path");
    g_index_path = pt.get<string>("all.index_path");
    g_db_path = pt.get<string>("all.db_path");
    g_rebuild = pt.get<bool>("all.rebuild");
    DATA_MAX_SIZE = pt.get<int>("all.data_max_size") * g_ch_size;
    SLICE_WRITE_SIZE = pt.get<int>("all.slice_write_size");
    g_offset = pt.get<int>("all.return_windows") * g_ch_size;
}

void get_slice_vec(const string & ch, vector< pair < int, long > > & vc) {
    vc.clear();
    if (g_char2slice_map.find(ch) == g_char2slice_map.end()) {
        return ;  // Not in
    }
    for(int i = 0; i <= g_char2slice_map[ch]; i++) {
        string keyName;
        compose_slice_key(ch, i, keyName);
        string value;
        if(get_db(keyName, value)) {
            vector<pair<int, long>> tv;
            stringstream ss(value);
            str_2_obj(ss, tv);
            vc.reserve( vc.size() + tv.size() );                // preallocate memory without erase original data
            vc.insert( vc.end(), tv.begin(), tv.end() );
        } else {
            LogFatal("No in db");
        }
    }
}

template <class type> void _compute_true_doc_pos(pair<int, long> & dat_pos, const type & pos) {
    auto true_pos = (dat_pos.second + pos * g_ch_size);
    auto doc_offset = true_pos / DATA_MAX_SIZE;
    dat_pos.second = true_pos % DATA_MAX_SIZE;
    dat_pos.first += doc_offset;
}


void _get_doc(pair<int, long> doc_pos, string & st, const int & len) {
    pair<int, long> dat_pos = g_doc2startpos_map[doc_pos.first];
    _compute_true_doc_pos(dat_pos, doc_pos.second);
    string filename;
    compose_data_name<int> (dat_pos.first, filename);
    long start_pos = dat_pos.second - (len - 1) * g_ch_size - g_offset;
    if (start_pos < 0) {
        start_pos = 0;
    }
    long end_pos = dat_pos.second + g_ch_size + g_offset;
    if (end_pos > DATA_MAX_SIZE) {
        end_pos = DATA_MAX_SIZE;
    }
    vector<unsigned short> vc;
    ifstream fin(filename);
    if (!fin) {
        LogFatal("Not open %s" , filename.c_str());
    }
    fin.seekg(start_pos);
    unsigned short ch;
    auto i = 0;
    while(fin.read(reinterpret_cast<char *>(& ch), g_ch_size)) {
        i += g_ch_size;
        if (i > end_pos - start_pos) {
            break;
        }
        vc.push_back(ch);
    }
    // cout << "Str len " << vc.size() <<" " <<start_pos  <<" : " <<end_pos <<endl;
    u16_2_u8(vc, st);
}

void _get_string_from_doc_pos(vector<pair<int, long> > doc_pos, vector<string> & sout, const int & len) {
    for (auto pos: doc_pos) {
        string st;
        _get_doc(pos, st, len);
        sout.push_back(st);
        // cout <<"Find " <<count <<" " << st <<endl;
    }
}

void _search(const vector<unsigned short> & utf16line, vector<string> & sout) {
    vector<pair<int, long>> A, B, C;
    bool first = true;
    unsigned short pre_ch = 0xFF;

    for (auto ch: utf16line) {
        string new_key;
        compose_two_key(pre_ch, ch, new_key);
        pre_ch = ch;
        if (first) {
            first = false;
            get_slice_vec(new_key, A);  // The first run
            continue;
        }
        if (A.size() == 0) {
            break; // Not found
        }
        get_slice_vec(new_key, B);
        find_section(A, B, C);
        A = C;
    }
    _get_string_from_doc_pos(A, sout, utf16line.size());
}

void search(string & line) {
    vector<unsigned short > utf16line;
    u8_2_u16(line, utf16line);
    vector<string> sout;
    _search(utf16line, sout);
    auto lineNum = 0;
    for ( auto i: sout) {
        lineNum++;
        cout <<"Line " <<lineNum <<": " << i <<endl;
    }
}
void search_loop() {
    while(true) {
        string sin;
        cout <<"Pls input word to search" <<endl;
        cin >> sin;
        search(sin);
    }
}


int main(int argc, char ** argv) {
    try {
        load_config(argv[1]);

        // g_InitLog();
        init_db(g_db_path, &g_db);

        if (g_rebuild) {
            build_index(g_index_path.c_str());
        }

        read_index(g_index_path.c_str());

        search_loop();

    } catch (exception& e) {
        LogFatal(e.what());
    }
}
