use crate::PeerNode;

/// Finds the neighbors of this node and wraps around the ring if necessary.
/// Does not return this node itself in any case.
/// If greater and smaller neighbour would be the same, it is only returned once.
pub fn find_neighbors_wrapping(
    this_node_id: u64,
    node_list: &Vec<PeerNode>,
) -> [Option<PeerNode>; 2] {
    // smaller neighbor = node with greatest ID smaller than self
    // if not found, use the greatest node
    let greatest_node = node_list
        .iter()
        .filter(|node| node.id != this_node_id)
        .max_by_key(|node| node.id);

    let smaller_neighbor = node_list
        .iter()
        .filter(|peer| peer.id < this_node_id)
        .max_by_key(|peer| peer.id);

    let smaller_neighbor = match (smaller_neighbor, greatest_node) {
        (Some(neighbor), _) => Some(neighbor.clone()),
        (None, greatest_node) => greatest_node.cloned(),
    };

    // greater neighbor = node with smallest ID greater than self
    // if not found, use the smallest node
    let smallest_node = node_list
        .iter()
        .filter(|node| node.id != this_node_id)
        .min_by_key(|node| node.id);

    let greater_neighbor = node_list
        .iter()
        .filter(|peer| peer.id > this_node_id)
        .min_by_key(|peer| peer.id);

    let greater_neighbor = match (greater_neighbor, smallest_node) {
        (Some(neighbor), _) => Some(neighbor.clone()),
        (None, smallest_node) => smallest_node.cloned(),
    };

    // do not return the same node twice
    match (smaller_neighbor, greater_neighbor) {
        (Some(smaller), Some(greater)) => {
            if smaller.id == greater.id {
                [Some(smaller), None]
            } else {
                [Some(smaller), Some(greater)]
            }
        }
        (smaller, greater) => [smaller, greater],
    }
}

/// Finds the neighbors of this node and without wrapping around the ring.
/// Does not return this node itself in any case.
/// There is no case where the returned neighbors would be the same.
pub fn find_neighbors_nonwrapping(
    this_node_id: u64,
    node_list: &[PeerNode],
) -> (Option<PeerNode>, Option<PeerNode>) {
    // smaller neighbor = node with greatest ID smaller than self
    let smaller_neighbor = node_list
        .iter()
        .filter(|node| node.id < this_node_id)
        .max_by_key(|node| node.id);

    // greater neighbor = node with smallest ID greater than self
    let greater_neighbor = node_list
        .iter()
        .filter(|node| node.id > this_node_id)
        .min_by_key(|node| node.id);

    (smaller_neighbor.cloned(), greater_neighbor.cloned())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_neighbor_wrapping_finding() {
        let node_list = vec![
            PeerNode {
                id: 100,
                ip_address: "192.168.0.100".to_string(),
            },
            PeerNode {
                id: 200,
                ip_address: "192.168.0.200".to_string(),
            },
            PeerNode {
                id: 150,
                ip_address: "192.168.0.150".to_string(),
            },
            PeerNode {
                id: 10,
                ip_address: "192.168.0.10".to_string(),
            },
        ];
        let result = find_neighbors_wrapping(200, &node_list);
        assert_eq!(result[0].clone().unwrap().id, 150);
        assert_eq!(result[1].clone().unwrap().id, 10);
    }

    #[test]
    fn missing_neighbor_wrapping_finding() {
        let node_list = vec![
            PeerNode {
                id: 100,
                ip_address: "192.168.0.100".to_string(),
            },
            PeerNode {
                id: 200,
                ip_address: "192.168.0.200".to_string(),
            },
        ];
        let result = find_neighbors_wrapping(200, &node_list);
        assert_eq!(result[0].clone().unwrap().id, 100);
        assert!(result[1].is_none());
    }

    #[test]
    fn alone_neighbor_wrapping_finding() {
        let node_list = vec![PeerNode {
            id: 200,
            ip_address: "192.168.0.200".to_string(),
        }];
        let result = find_neighbors_wrapping(200, &node_list);
        assert!(result[0].is_none());
        assert!(result[1].is_none());
    }
}
