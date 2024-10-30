#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <tbb/global_control.h>
#include <unistd.h>

#include "contractor/graph_contractor.hpp"
#include "engine/datafacade.hpp"
#include "engine/datafacade/shared_memory_allocator.hpp"
#include "engine/routing_algorithms/routing_base.hpp"
#include "engine/routing_algorithms/routing_base_ch.hpp"
#include "engine/search_engine_data.hpp"
#include "util/get_time.hpp"
using namespace osrm;
using namespace osrm::contractor;

inline uint32_t hash32_2(uint32_t a) {
  uint32_t z = (a + 0x6D2B79F5UL);
  z = (z ^ (z >> 15)) * (z | 1UL);
  z ^= z + (z ^ (z >> 7)) * (z | 61UL);
  return z ^ (z >> 14);
}

using Edge = std::tuple<unsigned, unsigned, int>;
inline contractor::ContractorGraph makeGraph(const std::vector<Edge>& edges) {
  std::vector<contractor::ContractorEdge> input_edges;
  auto id = 0u;
  auto max_id = 0u;
  for (const auto& edge : edges) {
    unsigned start;
    unsigned target;
    int weight;
    std::tie(start, target, weight) = edge;
    int duration = weight * 2;
    float distance = 1.0;
    max_id = std::max(std::max(start, target), max_id);
    input_edges.push_back(contractor::ContractorEdge{
        start, target,
        contractor::ContractorEdgeData{
            {weight}, {duration}, {distance}, 0, ++id, false, true, false}});
    input_edges.push_back(contractor::ContractorEdge{
        target, start,
        contractor::ContractorEdgeData{
            {weight}, {duration}, {distance}, 0, ++id, false, false, true}});
  }
  std::sort(input_edges.begin(), input_edges.end());
  // for (auto edge : input_edges) {
  // assert(edge.data.originalEdges == 0);
  // assert(edge.data.id >= 1 && edge.data.id <= 2 * edges.size());
  //}
  // printf("Pass\n");

  return contractor::ContractorGraph{max_id + 1, input_edges};
}

inline unsigned int init_num_workers() {
  if (const auto env_p = std::getenv("TBB_NUM_THREADS")) {
    return std::stoi(env_p);
  } else {
    return std::thread::hardware_concurrency();
  }
}

std::pair<std::vector<uint32_t>, std::vector<std::pair<uint32_t, int32_t>>>
edges2graph(std::vector<std::tuple<uint32_t, uint32_t, int32_t>>& in_edges,
            size_t n) {
  std::sort(begin(in_edges), end(in_edges));
  std::vector<uint32_t> offsets(n);
  std::vector<std::pair<uint32_t, int32_t>> edges;
  uint32_t last_u = 0;
  for (auto [u, v, w] : in_edges) {
    // printf("(%u,%u,%d)\n", u, v, w);
    if (u != last_u) {
      for (uint32_t i = last_u + 1; i <= u; i++) {
        offsets[i] = edges.size();
      }
    }
    edges.push_back(std::make_pair(v, w));
    last_u = u;
  }
  for (size_t i = last_u + 1; i < n; i++) {
    offsets[i] = edges.size();
  }
  assert(offsets[0] == 0);
  return std::make_pair(offsets, edges);
}

int main(int argc, char* argv[]) {
  printf("num_threads: %u\n", init_num_workers());
  tbb::global_control scheduler(tbb::global_control::max_allowed_parallelism,
                                init_num_workers());
  if (argc < 2) {
    printf("Usage: %s <input_graph> <output_graph>\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  std::string filename(argv[1]);
  std::ifstream ifs(filename);
  std::vector<unsigned> offsets;
  std::vector<std::tuple<unsigned, unsigned, int>> edges;
  size_t n, m;
  if (filename.find(".adj") != std::string::npos) {
    printf("Reading pbbs format...\n");
    std::string header;
    ifs >> header;
    ifs >> n >> m;
    offsets = std::vector<unsigned>(n + 1);
    edges = std::vector<std::tuple<unsigned, unsigned, int>>(m);
    for (size_t i = 0; i < n; i++) {
      ifs >> offsets[i];
    }
    offsets[n] = m;
    for (size_t i = 0; i < n; i++) {
      for (size_t j = offsets[i]; j < offsets[i + 1]; j++) {
        get<0>(edges[j]) = i;
      }
    }
    for (size_t i = 0; i < m; i++) {
      ifs >> get<1>(edges[i]);
    }
    for (size_t i = 0; i < m; i++) {
      double w;
      ifs >> w;
      get<2>(edges[i]) = (int)w;
    }
  } else if (filename.find(".bin") != std::string::npos) {
    printf("Reading binary format...\n");
    struct stat sb;
    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
      std::cerr << "Error: Cannot open file " << argv[1] << std::endl;
      abort();
    }
    if (fstat(fd, &sb) == -1) {
      std::cerr << "Error: Unable to acquire file stat" << std::endl;
      abort();
    }
    char* data =
        static_cast<char*>(mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    size_t len = sb.st_size;
    n = reinterpret_cast<uint64_t*>(data)[0];
    m = reinterpret_cast<uint64_t*>(data)[1];
    offsets = std::vector<unsigned>(n + 1);
    edges = std::vector<std::tuple<unsigned, unsigned, int>>(m);
    for (size_t i = 0; i < n + 1; i++) {
      offsets[i] = reinterpret_cast<uint64_t*>(data + 3 * 8)[i];
    }
    constexpr int LOG2_WEIGHT = 5;  // 18
    constexpr int WEIGHT = 1 << LOG2_WEIGHT;
    for (size_t i = 0; i < n; i++) {
      for (size_t j = offsets[i]; j < offsets[i + 1]; j++) {
        unsigned u = i;
        unsigned v = reinterpret_cast<uint32_t*>(data + 3 * 8 + (n + 1) * 8)[j];
        int w = ((hash32_2(u) ^ hash32_2(v)) & (WEIGHT - 1)) + 1;
        edges[j] = std::make_tuple(u, v, w);
      }
    }
    if (data) {
      const void* b = data;
      munmap(const_cast<void*>(b), len);
    }
  } else {
    std::cerr << "Unsupported file extension\n";
    abort();
  }
  ifs.close();

  // contraction
  printf("n: %zu, m:%zu\n", n, m);
  auto reference_graph = makeGraph(edges);
  auto contracted_graph = reference_graph;
  std::vector<EdgeWeight> node_weights(n, EdgeWeight{1});
  timer t;
  contractGraph(contracted_graph, std::move(node_weights));
  t.stop();
  printf("Total time: %f\n", t.total_time());

  std::vector<std::tuple<uint32_t, uint32_t, int32_t>> forward;
  std::vector<std::tuple<uint32_t, uint32_t, int32_t>> backward;
  assert(n == contracted_graph.GetNumberOfNodes());
  size_t total_edges = contracted_graph.GetNumberOfEdges();
  for (uint32_t u = 0; u < n; u++) {
    for (auto edge : contracted_graph.GetAdjacentEdgeRange(u)) {
      const ContractorEdgeData& data = contracted_graph.GetEdgeData(edge);
      uint32_t v = contracted_graph.GetTarget(edge);
      int32_t w = from_alias<int32_t>(data.weight);
      // if (data.shortcut) {
      if (data.forward) {
        forward.push_back(std::make_tuple(u, v, w));
      } else {
        assert(data.backward);
        backward.push_back(std::make_tuple(u, v, w));
      }
      //}
    }
  }
  // printf("forward\n");
  auto [forward_offsets, forward_edges] = edges2graph(forward, n);
  // printf("backward\n");
  auto [backward_offsets, backward_edges] = edges2graph(backward, n);

  std::string output_file(argv[2]);
  std::ofstream ofs(output_file);
  size_t aaam = forward.size(), aaarm = backward.size();
  printf("m: %zu, rm: %zu\n", aaam, aaarm);
  if (total_edges != aaam + aaarm) {
    printf("Edges number not equal\n");
    fflush(stdout);
    exit(EXIT_FAILURE);
  }
  if (forward_offsets.size() != n) {
    printf("forward_offsets: %zu, n: %zu\n", forward_offsets.size(), n);
    fflush(stdout);
    exit(EXIT_FAILURE);
  }
  if (backward_offsets.size() != n) {
    printf("backward_offsets: %zu, n: %zu\n", backward_offsets.size(), n);
    fflush(stdout);
    exit(EXIT_FAILURE);
  }
  if (contracted_graph.rank.size() != n) {
    printf("rank: %zu, n: %zu\n", contracted_graph.rank.size(), n);
    fflush(stdout);
    exit(EXIT_FAILURE);
  }
  assert(total_edges == aaam + aaarm);
  printf("writing...\n");
  size_t layerOffset = 0, ccOffset = 0;
  ofs << "header\n";
  ofs << n << '\n' << aaam << '\n' << aaarm << '\n';
  ofs << layerOffset << '\n' << ccOffset << '\n';
  for (size_t i = 0; i < n; i++) {
    ofs << forward_offsets[i] << '\n';
  }
  for (size_t i = 0; i < n; i++) {
    ofs << contracted_graph.rank[i] << '\n';
  }
  for (size_t i = 0; i < aaam; i++) {
    ofs << forward_edges[i].first << '\n';
  }
  for (size_t i = 0; i < aaam; i++) {
    ofs << forward_edges[i].second << '\n';
  }
  for (size_t i = 0; i < n; i++) {
    ofs << backward_offsets[i] << '\n';
  }
  for (size_t i = 0; i < aaarm; i++) {
    ofs << backward_edges[i].first << '\n';
  }
  for (size_t i = 0; i < aaarm; i++) {
    ofs << backward_edges[i].second << '\n';
  }
  ofs.close();

  // using Algorithm = osrm::engine::routing_algorithms::ch::Algorithm;
  // using Facade =
  // osrm::engine::datafacade::ContiguousInternalMemoryDataFacade<Algorithm>;
  // osrm::engine::SearchEngineData<Algorithm> heaps;
  // std::string metric_name;
  // auto facade =
  // Facade(std::make_shared<osrm::engine::datafacade::SharedMemoryAllocator>(),
  // metric_name, 0);
  return 0;
}
