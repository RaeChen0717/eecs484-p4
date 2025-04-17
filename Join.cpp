#include "Join.hpp"
#include "constants.hpp"
#include "Record.hpp"
#include <vector>
using namespace std;
/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	const uint num_part = MEM_SIZE_IN_PAGE - 1;
    vector<Bucket> partitions;
    partitions.reserve(num_part);
    for (uint i = 0; i < num_part; ++i) {
        partitions.emplace_back(disk);
    }
    const uint in_buf = num_part;
    // --- Partition left relation ---
    mem->reset();
    for (uint pid = left_rel.first; pid < left_rel.second; ++pid) {
        mem->loadFromDisk(disk, pid, in_buf);
        Page* page = mem->mem_page(in_buf);
        for (uint i = 0; i < page->size(); ++i) {
            Record rec = page->get_record(i);
            uint h = rec.partition_hash() % num_part;
            Page* out = mem->mem_page(h);
            out->loadRecord(rec);
            if (out->full()) {
                uint out_pid = mem->flushToDisk(disk, h);
                partitions[h].add_left_rel_page(out_pid);
            }
        }
    }
    for (uint b = 0; b < num_part; ++b) {
        Page* out = mem->mem_page(b);
        if (!out->empty()) {
            uint out_pid = mem->flushToDisk(disk, b);
            partitions[b].add_left_rel_page(out_pid);
        }
    }
    // --- Partition right relation ---
    mem->reset();
    for (uint pid = right_rel.first; pid < right_rel.second; ++pid) {
        mem->loadFromDisk(disk, pid, in_buf);
        Page* page = mem->mem_page(in_buf);
        for (uint i = 0; i < page->size(); ++i) {
            Record rec = page->get_record(i);
            uint h = rec.partition_hash() % num_part;
            Page* out = mem->mem_page(h);
            out->loadRecord(rec);
            if (out->full()) {
                uint out_pid = mem->flushToDisk(disk, h);
                partitions[h].add_right_rel_page(out_pid);
            }
        }
    }
    for (uint b = 0; b < num_part; ++b) {
        Page* out = mem->mem_page(b);
        if (!out->empty()) {
            uint out_pid = mem->flushToDisk(disk, b);
            partitions[b].add_right_rel_page(out_pid);
        }
    }
	return partitions;
}
/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> result;
    const uint num_ht = MEM_SIZE_IN_PAGE - 2;
    const uint in_buf = MEM_SIZE_IN_PAGE - 1;
    const uint out_buf = 0;

    // reset memory; page 0 used for output
    mem->reset();
    Page* out_page = mem->mem_page(out_buf);

    for (auto& bucket : partitions) {
        // clear hash buckets (1..num_ht) and input buffer
        for (uint pid = 1; pid <= num_ht; ++pid) {
            mem->mem_page(pid)->reset();
        }
        mem->mem_page(in_buf)->reset();

        // choose build and probe pages (copy vectors)
        auto build_pages = bucket.num_left_rel_record <= bucket.num_right_rel_record
            ? bucket.get_left_rel() : bucket.get_right_rel();
        auto probe_pages = bucket.num_left_rel_record <= bucket.num_right_rel_record
            ? bucket.get_right_rel() : bucket.get_left_rel();

        // build hash table
        for (uint pid : build_pages) {
            mem->loadFromDisk(disk, pid, in_buf);
            Page* p = mem->mem_page(in_buf);
            for (uint i = 0; i < p->size(); ++i) {
                Record rec = p->get_record(i);
                uint h2 = rec.probe_hash() % num_ht;
                mem->mem_page(1 + h2)->loadRecord(rec);
            }
        }

        // probe
        for (uint pid : probe_pages) {
            mem->loadFromDisk(disk, pid, in_buf);
            Page* p = mem->mem_page(in_buf);
            for (uint i = 0; i < p->size(); ++i) {
                Record pr = p->get_record(i);
                uint h2 = pr.probe_hash() % num_ht;
                Page* hpage = mem->mem_page(1 + h2);
                for (uint j = 0; j < hpage->size(); ++j) {
                    Record br = hpage->get_record(j);
                    if (br == pr) {
                        if (bucket.num_left_rel_record <= bucket.num_right_rel_record)
                            out_page->loadPair(br, pr);
                        else
                            out_page->loadPair(pr, br);
                        if (out_page->full()) {
                            uint out_pid = mem->flushToDisk(disk, out_buf);
                            result.push_back(out_pid);
                        }
                    }
                }
            }
        }
    }

    // final flush of any remaining results
    if (!out_page->empty()) {
        uint out_pid = mem->flushToDisk(disk, out_buf);
        result.push_back(out_pid);
    }

    return result;
}