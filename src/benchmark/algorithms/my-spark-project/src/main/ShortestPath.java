import java.util.*;

public class ShortestPath {
    private static final int INF = Integer.MAX_VALUE;

    public static Map<Integer, Integer> dijkstra(int[][] graph, int source) {
        int V = graph.length;
        Map<Integer, Integer> dist = new HashMap<>();
        Set<Integer> visited = new HashSet<>();

        for (int i = 0; i < V; i++) {
            dist.put(i, INF);
        }

        dist.put(source, 0);

        for (int i = 0; i < V - 1; i++) {
            int u = minDistance(dist, visited);
            visited.add(u);
            for (int v = 0; v < V; v++) {
                if (!visited.contains(v) && graph[u][v] != 0 && dist.get(u) != INF &&
                        dist.get(u) + graph[u][v] < dist.get(v)) {
                    dist.put(v, dist.get(u) + graph[u][v]);
                }
            }
        }

        return dist;
    }

    private static int minDistance(Map<Integer, Integer> dist, Set<Integer> visited) {
        int min = INF;
        int minIndex = -1;
        for (Map.Entry<Integer, Integer> entry : dist.entrySet()) {
            int v = entry.getKey();
            int d = entry.getValue();
            if (!visited.contains(v) && d < min) {
                min = d;
                minIndex = v;
            }
        }
        return minIndex;
    }

    public static void main(String[] args) {
        int[][] graph = {
                {0, 4, 0, 0, 0, 0, 0, 8, 0},
                {4, 0, 8, 0, 0, 0, 0, 11, 0},
                {0, 8, 0, 7, 0, 4, 0, 0, 2},
                {0, 0, 7, 0, 9, 14, 0, 0, 0},
                {0, 0, 0, 9, 0, 10, 0, 0, 0},
                {0, 0, 4, 14, 10, 0, 2, 0, 0},
                {0, 0, 0, 0, 0, 2, 0, 1, 6},
                {8, 11, 0, 0, 0, 0, 1, 0, 7},
                {0, 0, 2, 0, 0, 0, 6, 7, 0}
        };

        int source = 0;
        Map<Integer, Integer> dist = dijkstra(graph, source);

        System.out.println("Shortest distances from vertex " + source + " to other vertices:");
        for (Map.Entry<Integer, Integer> entry : dist.entrySet()) {
            System.out.println("Vertex " + entry.getKey() + ": Distance = " + entry.getValue());
        }
    }
}
