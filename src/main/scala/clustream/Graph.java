package clustream;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Graph {
    // Number of vertex
    int v;

    // Adjacency matrix
    int[][] adj;

    // Function to fill the empty
    // adjacency matrix
    Graph(int v) {
        this.v = v;

        adj = new int[v][v];
        for (int row = 0; row < v; row++)
            Arrays.fill(adj[row], 0);
    }

    // Function to add an edge to the graph
    void addEdge(int start, int e) {

        // Considering a bidirectional edge
        adj[start][e] = 1;
        adj[e][start] = 1;
    }

    // Function to perform BFS on the graph
    List<Integer> BFS(int start) {

        // Visited vector to so that
        // a vertex is not visited more than once
        // Initializing the vector to false as no
        // vertex is visited at the beginning
        boolean[] visited = new boolean[v];
        Arrays.fill(visited, false);
        List<Integer> q = new ArrayList<>();
        q.add(start);

        // Set source as visited
        visited[start] = true;

        int vis;
        List<Integer> list = new ArrayList<>();
        while (!q.isEmpty()) {
            vis = q.get(0);

            // Print the current node
            list.add(vis);
            q.remove(q.get(0));

            // For every adjacent vertex to
            // the current vertex
            for (int i = 0; i < v; i++) {
                if (adj[vis][i] == 1 && (!visited[i])) {

                    // Push the adjacent node to
                    // the queue
                    q.add(i);

                    // Set
                    visited[i] = true;
                }
            }
        }
        return list;
    }

    public List<Row> getResult(Graph g) {
        Set<List<Integer>> set = new HashSet<>();

        //Set i equals to max number + 1 in the graph
        for (int i = 0; i < v; i++) {
            List<Integer> bfs = g.BFS(i);
            if (bfs.size() == 1)
                continue;
            bfs.sort(Comparator.naturalOrder());
            set.add(bfs);
        }

        List<Row> rows = new ArrayList<>();

        AtomicInteger atomicInteger = new AtomicInteger(1000);
        set.forEach(integers -> {
          Integer incNum=atomicInteger.getAndIncrement();
            for (int i = 0; i < integers.size(); i++) {
              Row r=  RowFactory.create(incNum,integers.get(i));
                rows.add(r);
            }
        });
        return rows;
    }


}
