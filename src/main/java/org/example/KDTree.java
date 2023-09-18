package org.example;

import lombok.Getter;
import org.example.data.LatLng;

import java.util.*;

class KDTree {
  private Node root;
  private final int K = 2; // 2D tree
  private Map<String, double[]> labelToPointMap;

  public KDTree() {
    this.labelToPointMap = new HashMap<>();
  }

  // Static class to represent a node
  static class Node {
    double[] point;
    String label;
    Node left;
    Node right;

    public Node(String label, double[] point) {
      this.label = label;
      this.point = point;
    }
  }

  // Method to insert a new node
  public void insert(String label, double[] point) {
    root = insertRec(root, label, point, 0);
    labelToPointMap.put(label, point);
  }

  // Recursive method to insert a new node
  private Node insertRec(Node root, String label, double[] point, int depth) {
    if (root == null) {
      return new Node(label, point);
    }

    int cd = depth % K;
    if (point[cd] < root.point[cd]) {
      root.left = insertRec(root.left, label, point, depth + 1);
    } else {
      root.right = insertRec(root.right, label, point, depth + 1);
    }

    return root;
  }

  // Method to delete a node
  public void delete(String label) {
    double[] point = labelToPointMap.get(label);
    if (point != null) {
      root = deleteRec(root, point, 0);
      labelToPointMap.remove(label);
    }
  }

  // Recursive method to delete a node
  private Node deleteRec(Node root, double[] point, int depth) {
    if (root == null) {
      return null;
    }

    int cd = depth % K;

    if (Arrays.equals(point, root.point)) {
      // If right child is not null
      if (root.right != null) {
        Node min = findMin(root.right, cd, 0);
        root.point = min.point;
        root.label = min.label;
        root.right = deleteRec(root.right, min.point, depth + 1);
      } else if (root.left != null) {
        Node min = findMin(root.left, cd, 0);
        root.point = min.point;
        root.label = min.label;
        root.right = deleteRec(root.left, min.point, depth + 1);
        root.left = null;
      } else {
        return null;
      }
      return root;
    }

    if (point[cd] < root.point[cd]) {
      root.left = deleteRec(root.left, point, depth + 1);
    } else {
      root.right = deleteRec(root.right, point, depth + 1);
    }

    return root;
  }

  // Method to find the minimum node
  private Node findMin(Node root, int d, int depth) {
    if (root == null) {
      return null;
    }

    int cd = depth % K;

    if (cd == d) {
      if (root.left == null) {
        return root;
      } else {
        return findMin(root.left, d, depth + 1);
      }
    }

    Node left = findMin(root.left, d, depth + 1);
    Node right = findMin(root.right, d, depth + 1);
    Node min = root;

    if (left != null && left.point[d] < min.point[d]) {
      min = left;
    }
    if (right != null && right.point[d] < min.point[d]) {
      min = right;
    }

    return min;
  }

  // Method to search a point by label
  public boolean search(String label) {
    return labelToPointMap.containsKey(label);
  }

  // Method to find n nearest neighbors
  public NearestNeighborList nearestNeighbors(double[] target, int n) {
    NearestNeighborList nnl = new NearestNeighborList(n);
    searchNearest(root, target, 0, nnl);
    return nnl;
  }

  // Recursive method to find n nearest neighbors
  private void searchNearest(Node root, double[] target, int depth, NearestNeighborList nnl) {
    if (root == null) {
      return;
    }

    int cd = depth % K;
    Node next;
    Node opposite;

    if (target[cd] < root.point[cd]) {
      next = root.left;
      opposite = root.right;
    } else {
      next = root.right;
      opposite = root.left;
    }

    searchNearest(next, target, depth + 1, nnl);

    double d = Distance.between(target, root.point);
    nnl.insert(root, d);

    // Suboptimal as depending on the distance in the current dimension, we may not need to search
    // the opposite branch
    searchNearest(opposite, target, depth + 1, nnl);
  }

  // Helper class to hold nearest neighbors
  static class NearestNeighborList {
    private final int maxSize;
    @Getter
    private final List<Node> nodes;
    @Getter
    private final List<Double> distances;


    public NearestNeighborList(int maxSize) {
      this.maxSize = maxSize;
      this.nodes = new ArrayList<>();
      this.distances = new ArrayList<>();
    }

    public void insert(Node node, double distance) {
      for (int i = 0; i < distances.size(); i++) {
        if (distance < distances.get(i)) {
          distances.add(i, distance);
          nodes.add(i, node);

          if (nodes.size() > maxSize) {
            nodes.remove(maxSize);
            distances.remove(maxSize);
          }

          return;
        }
      }

      if (nodes.size() < maxSize) {
        distances.add(distance);
        nodes.add(node);
      }
    }

    public double getMaxDistance() {
      if (distances.isEmpty()) {
        return Double.MAX_VALUE;
      }
      return distances.get(distances.size() - 1);
    }

  }

  public static void main(String[] args) {
    KDTree kdTree = new KDTree();

    double[] point1 = {3.5, 6.7};
    double[] point2 = {17.2, 15.8};
    double[] point3 = {13.9, 15.6};
    double[] point4 = {6.1, 12.3};
    double[] point5 = {9.4, 1.7};
    double[] point6 = {2.2, 7.8};
    double[] point7 = {10.5, 19.3};

    kdTree.insert("point1", point1);
    kdTree.insert("point2", point2);
    kdTree.insert("point3", point3);
    kdTree.insert("point4", point4);
    kdTree.insert("point5", point5);
    kdTree.insert("point6", point6);
    kdTree.insert("point7", point7);

    double[] target = {17.0, 15.0};
    List<String> nearestNeighbors = kdTree.nearestNeighbors(target, 3).getNodes().stream().map(node -> node.label).toList();

    System.out.println("Nearest neighbors to (" + target[0] + ", " + target[1] + "):");
    for (String label : nearestNeighbors) {
      System.out.println(label);
    }

    kdTree.delete("point2");

    nearestNeighbors = kdTree.nearestNeighbors(target, 3).getNodes().stream().map(node -> node.label).toList();

    System.out.println("Nearest neighbors to (" + target[0] + ", " + target[1] + ") after deletion:");
    for (String label : nearestNeighbors) {
      System.out.println(label);
    }
  }
}

