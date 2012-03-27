package com.github.cwilper.fcrepo.store.jcr;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;

/**
 * Iterates Fedora object nodes in a two-level JCR directory structure.
 */
class ObjectNodeIterator extends AbstractIterator<Node>{
    private static final Logger logger =
            LoggerFactory.getLogger(ObjectNodeIterator.class);
    private NodeIterator topIterator, midIterator, bottomIterator;
    private Node topNode, midNode;

    ObjectNodeIterator(Node root) throws RepositoryException {
        topIterator = root.getNodes();
        nextTopNode();
    }
    
    private void nextTopNode() throws RepositoryException {
        topNode = nextDirNode(topIterator);
        if (topNode != null) {
            midIterator = topNode.getNodes();
            midNode = nextDirNode(midIterator);
            if (midNode != null) {
                bottomIterator = midNode.getNodes();
            }
        }
    }

    @Override
    protected Node computeNext() {
        try {
            while (topNode != null) {
                while (midNode != null) {
                    if (bottomIterator.hasNext()) {
                        return bottomIterator.nextNode();
                    }
                    midNode = nextDirNode(midIterator);
                    if (midNode != null) {
                        bottomIterator = midNode.getNodes();
                    }
                }
                nextTopNode();
            }
        } catch (RepositoryException e) {
            logger.warn("Error iterating JCR nodes; stopping early", e);
        }
        return endOfData();
    }
    
    private static Node nextDirNode(NodeIterator iterator)
            throws RepositoryException {
        while (iterator.hasNext()) {
            Node node = iterator.nextNode();
            if (node.getName().length() == 2) {
                return node;
            }
        }
        return null;
    }
}
