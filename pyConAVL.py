# Concurrent AVL tree implemented by Gabriel Siqueira and Yanju Chen from the University of California, Santa Barbara
# Implementation based on the paper A Practical Concurrent Binary Search Tree by Nathan Bronson
# Reference: https://ppl.stanford.edu/papers/ppopp207-bronson.pdf

from graphviz import Digraph
from IPython.display import Image, display
import threading
import random

# Node condition codes

UNLINK = -1
REBALANCE = -2
NOTHING = -3

class CC_RETRY(object):
    """
    Concurrent Control RETRY type:
    usually happen in __getNode when parent node version changes
    and retry should be performed from the caller function (with an updated dversion)
    """
    pass

class CC_UNLINKED(object):
    pass

class ConAVL(object):

    def __init__(self, simulate=False):
        """
        Initializes the ConAVL object. The root is set as an empty node. root.right will point to the actual root.
        """
        self.root = Node(None)
        self.simulate = simulate

    def get(self, key):
        """
        Returns the value of the node with corresponding key.
        """
        return self.__getNode(self.root, key)

    def put(self, key, val = None):
        """
        This method can be used in three ways:
        When only key is sent, and key is unique, it creates a new node where the value equals the key
        When key and value are sent and key is unique, it creates a new node with given key and given value
        When key and value are sent and key is already present, it updates the node with given key to the given value
        """
        self.__putNode(key, str(key) if val is None else val, self.root)

    def remove(self, dkey):
        """
        Removes the node with given key. The actual node might not be deleted from the tree. (see reference paper)
        """
        self.__putNode(dkey, None, self.root)

    def print(self):
        """
        Prints the underlying tree in a nice way.
        """
        self.__prettyPrintTree(self.root)
        
    def __str__(self):
        """
        Returns string representation of the tree.
        """
        return strTree(self.root.right)

    def __getNode(self, root, key):
        """
        If key is present, return the value,
        otherwise, return None
        """

        def attemptGet(self, key, node, dir, version):
            while True:
                cnode = node.getChild(dir)
                if cnode is None:
                    if node.version != version:
                        return CC_RETRY
                    return None
                else:
                    if key - cnode.key == 0:
                        return cnode.val
                    cversion = cnode.version
                    if cversion.shrinking or cversion.unlinked:
                        self.__waitUntilShrinkCompleted(cnode, cversion)
                        if node.version != version:
                            return CC_RETRY
                        # else: RETRY
                    elif cnode is not node.getChild(dir):
                        if node.version != version:
                            return CC_RETRY
                        # else: RETRY
                    else:
                        if node.version != version:
                            return CC_RETRY
                        vo = attemptGet(self, key, cnode, key - cnode.key, cversion)
                        if vo != CC_RETRY:
                            return vo
                        # else: RETRY

        while True:
            right = root.right
            if right is None:
                return None
            else:
                if key - right.key == 0:
                    return right.val
                cversion = right.version
                if cversion.shrinking or cversion.unlinked:
                    self.__waitUntilShrinkCompleted(right, cversion)
                    # and then RETRY
                    continue
                elif right is root.right:
                    # still the same cnode, not changing
                    vo = attemptGet(self, key, right, key - right.key, cversion)
                    if vo != CC_RETRY:
                        return vo
                    # else: RETRY
                else:
                    # RETRY
                    continue

    def __putNode(self, key, newValue, root):
        """
        Can be used to insert, update or remove a node.
        """
        
        # helper function of __putNode
        def attemptInsertIntoEmpty(key, newValue, root):
            """
            Inserts when the tree is empty.
            """
            with root.lock:
                if root.right is None:
                    root.right = Node(key, newValue, root)
                    root.height = 2
                    result = True
                else:
                    result = False
            return result
        
        # helper function of __putNode
        def attemptUpdate(key, newValue, parent, node, version):
            """
            Inserts new node or updates old value.
            """
            cmp = key - node.key
            if cmp == 0:
                return attemptNodeUpdate(newValue, parent, node)
            while True:
                child = node.getChild(cmp)
                if node.version != version:
                    return CC_RETRY
                
                if child is None:
                    # key is not present
                    if newValue is None:
                        # removal is requested
                        return None
                    else:
                        # update will be an insert
                        with node.lock:
                            if node.version != version:
                                return CC_RETRY
                            if node.getChild(cmp) is not None:
                                # lost a race with a concurrent insert
                                # must retry in the outer loop
                                success = False
                                damaged = None
                                # will RETRY
                            else:
                                if cmp <= -1:
                                    fakeConflict(self)
                                    node.left = Node(key, val=newValue, parent=node)
                                else:
                                    # key>root.key
                                    fakeConflict(self)
                                    node.right = Node(key, val=newValue, parent=node)

                                success = True
                                damaged = self.__fixHeight(node)

                        if success == True:
                            self.__fixHeightAndRebalance(damaged)
                            return None
                        # else: RETRY
                else:
                    cversion = child.version
                    if cversion.shrinking or cversion.unlinked:
                        self.__waitUntilShrinkCompleted(child, cversion)
                        # and then RETRY
                    elif child is not node.getChild(cmp):
                        continue # which is RETRY
                    else:
                        if node.version != version:
                            return CC_RETRY
                        vo = attemptUpdate(key, newValue, node, child, cversion)
                        if vo != CC_RETRY:
                            return vo
                        # else: RETRY
           
        # helper function of __putNode
        def attemptNodeUpdate(newValue, parent, node):
            """
            Updates node value.
            """
            if newValue is None:
                # removal
                if node.val is None:
                    # already removed, nothing to do
                    return None
            
            if newValue is None and (node.left is None or node.right is None):
                # potential unlink, get ready by locking the parent
                with parent.lock:
                    if parent.version.unlinked or node.parent != parent:
                        return CC_RETRY
                    with node.lock:
                        prev = node.val
                        if prev is None:
                            return prev
                        if not self.__attemptUnlink(parent, node):
                            return CC_RETRY
                    damaged = self.__fixHeight(parent)
                self.__fixHeightAndRebalance(damaged)
                return prev
            else:
                with node.lock:
                    if node.version.unlinked:
                        return CC_RETRY
                    prev = node.val
                    # retry if we now detect that unlink is possible
                    if newValue is None and (node.left is None or node.right is None):
                        return CC_RETRY
                    node.val = newValue
                return prev
            
        # =============================================================================
        while True:
            right = root.right
            fakeConflict(self)
            if right is None:
                # key is not present
                if newValue is None or attemptInsertIntoEmpty(key, newValue, root):
                    return None
                # else: RETRY
            else:
                cversion = right.version
                if cversion.shrinking or cversion.unlinked:
                    self.__waitUntilShrinkCompleted(right, cversion)
                    # and then RETRY
                elif right is root.right:
                    fakeConflict(self)
                    vo = attemptUpdate(key, newValue, root, right, cversion)
                    if vo != CC_RETRY:
                        return vo
                    # else: RETRY
                else:
                    continue # RETRY
        
    def __attemptUnlink(self, parent, node):
        """
        Tries to unlink a node that should have already been removed.
        """
        parentL = parent.left
        parentR = parent.right
        if parentL != node and parentR != node:
            # node is no longer a child of parent
            return False

        left = node.left
        right = node.right
        if (left is not None) and (right is not None):
            # splicing is no longer possible
            return False

        splice = left if (left is not None) else right
        if parentL == node:
            parent.left = splice
        else:
            parent.right = splice
        if splice is not None:
            splice.parent = parent

        # node.version = CC_UNLINKED
        node.version.unlinked = True
        node.val = None

        return True
    
    def __waitUntilShrinkCompleted(self, node, version):
        """
        Waits for lock to be released.
        """
        if not version.shrinking:
            # version changed
            return

        # Makes it wait for a while
        for i in range(100):
            if node.version != version:
                return

        node.lock.acquire()
        node.lock.release()

        assert node.version != version
        return

    def __fixHeightAndRebalance(self, node):
        """
        Recursively rebalances and fixes the height of nodes, climbing up the three.
        """
        while node is not None and node.parent is not None:
            c = self.__nodeCondition(node)
            if c == NOTHING or node.version.unlinked == True:
                # node is fine, or node isn't repairable
                return

            if c is not UNLINK and c is not REBALANCE:
                with node.lock:
                    node = self.__fixHeight(node)

            else:
                nodeParent = node.parent
                with nodeParent.lock:
                    if nodeParent.version.unlinked == False and node.parent == nodeParent:
                        with node.lock:
                            node = self.__rebalanceNode(nodeParent, node)

    def __fixHeight(self, node):
        """
        Attempts to fix the height of a node. Returns the lowest damaged node that the current thread is responsible for or null if no damaged nodes are found.
        """
        cond = self.__nodeCondition(node)
        if cond == REBALANCE:
            # Need to rebalance
            pass
        elif cond == UNLINK:
            # Need to unlink
            return node
        elif cond == NOTHING:
            # This thread doesn't need to do anything
            return None
        else:
            # Fix height, return parent which will need fixing next
            node.height = cond
            return node.parent

    def __nodeCondition(self, node):
        """
        Returns whether a node needs to be fixed (the correct height) or other status codes.
        """

        nodeLeft = node.left
        nodeRight = node.right

        if (nodeLeft is None or nodeRight is None) and node.val is None:
            # Need to unlink
            return UNLINK

        heightNode = node.height
        oldHeightLeft = 0 if nodeLeft is None else nodeLeft.height
        oldHeightRight = 0 if nodeRight is None else nodeRight.height

        newHeightNode = max(oldHeightLeft, oldHeightRight) +1

        if oldHeightLeft - oldHeightRight > 1 or oldHeightLeft - oldHeightRight < -1:
            # Need to rebalance
            return REBALANCE

        # No action needed
        return newHeightNode if heightNode != newHeightNode else NOTHING

    def __rebalanceNode(self, nodeParent, node):
        nodeRight = node.left
        nodeLeft = node.right
        if (nodeRight is None or nodeLeft is None) and node.val is None:
            if self.__attemptUnlink(nodeParent, node):
                # fix parent height
                return self.__fixHeight(nodeParent)
            return node
        heightNode = node.height
        oldHeightLeft = 0 if nodeRight is None else nodeRight.height
        oldHeightRight = 0 if nodeLeft is None else nodeLeft.height

        newHeightNode = max(oldHeightLeft, oldHeightRight) +1
        if oldHeightLeft - oldHeightRight < -1:
            return self.__rebalanceLeft(nodeParent, node, nodeLeft, oldHeightLeft)
        elif oldHeightLeft - oldHeightRight > 1:
            return self.__rebalanceRight(nodeParent, node, nodeRight, oldHeightRight)
        elif heightNode == newHeightNode:
            return None
        else:
            # fix height and fix the parent
            node.height = newHeightNode
            return self.__fixHeight(nodeParent)

    def __rebalanceLeft(self, nodeParent, node, nodeRight, oldHeightLeft):
        with nodeRight.lock:
            heightRight = nodeRight.height
            if oldHeightLeft - heightRight >= -1:
                # retry
                return node
            else:
                nodeRightLeft = nodeRight.left
                oldHeightRightLeft = 0 if nodeRightLeft is None else nodeRightLeft.height
                oldHeightRightRight = 0 if nodeRight.right is None else nodeRight.right.height
                if oldHeightRightRight >= oldHeightRightLeft:
                    return self.__rotateLeft(nodeParent, node, oldHeightLeft, nodeRight, nodeRightLeft, oldHeightRightLeft, oldHeightRightRight)
                else:
                    with nodeRightLeft.lock:
                        heightRightLeft = nodeRightLeft.height
                        if oldHeightRightRight >= heightRightLeft:
                            return self.__rotateLeft(nodeParent, node, oldHeightLeft, nodeRight, nodeRightLeft, heightRightLeft, oldHeightRightRight)
                        else:
                            heightRightLeftRight = 0 if nodeRightLeft.right is None else nodeRightLeft.right.height
                            if (oldHeightRightRight - heightRightLeftRight >= -1 and  oldHeightRightRight - heightRightLeftRight <= 1)and not ((oldHeightRightRight == 0 or heightRightLeftRight == 0) and nodeRight.val is None):
                                return self.__rotateLeftOverRight(nodeParent, node, oldHeightLeft, nodeRight, nodeRightLeft, oldHeightRightRight, heightRightLeftRight)
                    return self.__rebalanceRight(node, nodeRight, nodeRightLeft, oldHeightRightRight)

    def __rebalanceRight(self, nodeParent, node, nodeLeft, oldHeightRight):
        with nodeLeft.lock:
            heightLeft = nodeLeft.height
            if heightLeft - oldHeightRight <= 1:
                #retry
                return node
            else:
                nodeLeftRight = nodeLeft.right
                oldHeightLeftRight = 0 if nodeLeftRight is None else nodeLeftRight.height
                oldHeightLeftLeft = 0 if nodeLeft.left is None else nodeLeft.left.height
                if oldHeightLeftLeft >= oldHeightLeftRight:
                    return self.__rotateRight(nodeParent, node, oldHeightRight, nodeLeft, nodeLeftRight, oldHeightLeftRight, oldHeightLeftLeft)
                else:
                    with nodeLeftRight.lock:
                        heightLeftRight = nodeLeftRight.height
                        if oldHeightLeftLeft >= heightLeftRight:
                            return self.__rotateRight(nodeParent, node, oldHeightRight, nodeLeft, nodeLeftRight, heightLeftRight, oldHeightLeftLeft)
                        else:
                            heightLeftRightLeft = 0 if nodeLeftRight.left is None else nodeLeftRight.left.height
                            if (oldHeightLeftLeft - heightLeftRightLeft >= -1 and oldHeightLeftLeft - heightLeftRightLeft <= 1) and not ((oldHeightLeftLeft == 0 or heightLeftRightLeft == 0) and nodeLeft.val is None):
                                return self.__rotateRightOverLeft(nodeParent, node, oldHeightRight, nodeLeft, nodeLeftRight, oldHeightLeftLeft, heightLeftRightLeft)
                    return self.__rebalanceLeft(node, nodeLeft, nodeLeftRight, oldHeightLeftLeft)

    def __rotateLeftOverRight(self, nodeParent, node, heightLeft, nodeRight, nodeRightLeft, heightRightRight, heightRightLeftRight):

        nodeParentLeft = nodeParent.left
        nodeRightLeftLeft = nodeRightLeft.left
        nodeRightLeftRight = nodeRightLeft.right
        heightRightLeftRight = 0 if nodeRightLeftLeft is None else nodeRightLeftLeft.height

        node.version.shrinking = True
        nodeRight.version.shrinking = True

        # Fix all the pointers
        node.right = nodeRightLeftLeft
        if nodeRightLeftLeft is not None:
            nodeRightLeftLeft.parent = node

        nodeRight.left = nodeRightLeftRight
        if nodeRightLeftRight is not None:
            nodeRightLeftRight.parent = nodeRight

        nodeRightLeft.right = nodeRight
        nodeRight.parent = nodeRightLeft
        nodeRightLeft.left = node
        node.parent = nodeRightLeft

        if nodeParentLeft != node:
            nodeParent.right = nodeRightLeft
        else:
            nodeParent.left = nodeRightLeft
        nodeRightLeft.parent = nodeParent

        # Fix all the heights
        newNodeHeight = max(heightRightLeftRight, heightLeft) + 1
        node.height = newNodeHeight
        newNodeRightHeight = max(heightRightRight, heightRightLeftRight) + 1
        nodeRight.height = newNodeRightHeight
        nodeRightLeft.height = max(newNodeHeight, newNodeRightHeight) + 1

        node.version.shrinking = False
        node.version.number += 1
        nodeRight.version.shrinking = False
        node.version.number += 1

        assert abs(heightRightRight - heightRightLeftRight) <= 1

        if (heightRightLeftRight - heightLeft < -1 or heightRightLeftRight - heightLeft > 1) or ((nodeRightLeftLeft is None or heightLeft == 0) and node.val is None):
            return node

        if (newNodeRightHeight - newNodeHeight < -1 or newNodeRightHeight - newNodeHeight > 1) :
            return nodeRightLeft
        return self.__fixHeight(nodeParent)

    def __rotateRightOverLeft(self, nodeParent, node, heightRight, nodeLeft, nodeLeftRight, heightLeftLeft, heightLeftRightLeft):

        nodeParentLeft = nodeParent.left
        nodeLeftRightLeft = nodeLeftRight.left
        nodeLeftRightRight = nodeLeftRight.right
        heightLeftRightRight = 0 if nodeLeftRightRight is None else nodeLeftRightRight.height

        node.version.shrinking = True
        nodeLeft.version.shrinking = True

        # Fix all the pointers

        node.left = nodeLeftRightRight
        if nodeLeftRightRight is not None:
            nodeLeftRightRight.parent = node

        nodeLeft.right = nodeLeftRightLeft
        if nodeLeftRightLeft is not None:
            nodeLeftRightLeft.parent = nodeLeft

        nodeLeftRight.left = nodeLeft
        nodeLeft.parent = nodeLeftRight
        nodeLeftRight.right = node
        node.parent = nodeLeftRight

        if nodeParentLeft != node:
            nodeParent.right = nodeLeftRight
        else:
            nodeParent.left = nodeLeftRight
        nodeLeftRight.parent = nodeParent

        # Fix all the heights
        newNodeHeight = max(heightLeftRightRight, heightRight) + 1
        node.height = newNodeHeight
        newNodeLeftHeight = max(heightLeftLeft, heightLeftRightLeft) + 1
        nodeLeft.height = newNodeLeftHeight
        nodeLeftRight.height = max(newNodeHeight, newNodeLeftHeight) + 1

        node.version.shrinking = False
        node.version.number += 1
        nodeLeft.version.shrinking = False
        nodeLeft.version.number += 1

        assert abs(heightLeftLeft - heightLeftRightLeft) <= 1
        assert not ((heightLeftLeft == 0 or nodeLeftRightLeft is None) and nodeLeft.val is None)

        if (heightLeftRightRight - heightRight < -1 or heightLeftRightRight - heightRight > 1) or ((nodeLeftRightRight is None or heightRight == 0) and node.val is None):
            return node

        if (newNodeLeftHeight - newNodeHeight < -1 or newNodeLeftHeight - newNodeHeight > 1) :
            return nodeLeftRight
        return self.__fixHeight(nodeParent)

    def __rotateLeft(self, nodeParent, node, heightRigh, nodeRight, nodeRightLeft, heightRightLeft, heightRightRight):
        nodeParentLeft = nodeParent.left # sibling or itself

        node.version.shrinking = True

        # Fix all the pointers
        node.right = nodeRightLeft
        if nodeRightLeft is not None:
            nodeRightLeft.parent = node

        nodeRight.left = node
        node.parent = nodeRight

        if nodeParentLeft is node:
            nodeParent.left = nodeRight
        else:
            nodeParent.right = nodeRight
        nodeRight.parent = nodeParent

        # Fix the heights
        newNodeHeight = max(heightRigh, heightRightLeft) + 1
        node.height = newNodeHeight
        nodeRight.height = max(heightRightRight, newNodeHeight) + 1

        node.version.shrinking = False
        node.version.number += 1

        if (heightRightLeft - heightRigh < -1 or heightRightLeft - heightRigh > 1) or ((nodeRightLeft is None or heightRigh == 0) and node.val is None):
            return node

        if (heightRightRight - newNodeHeight< -1 or heightRightRight - newNodeHeight > 1) or (heightRightRight == 0 and nodeRight.val is None):
            return nodeRight

        return self.__fixHeight(nodeParent)

    def __rotateRight(self, nodeParent, node, heightRight, nodeLeft, nodeLeftRight, heightLeftRight, heightLeftLeft):
        nodeParentLeft = nodeParent.left  # sibling or itself

        node.version.shrinking = True

        # Fix all the pointers
        node.left = nodeLeftRight
        if nodeLeftRight is not None:
            nodeLeftRight.parent = node

        nodeLeft.right = node
        node.parent = nodeLeft

        if nodeParentLeft is node:
            nodeParent.left = nodeLeft
        else:
            nodeParent.right = nodeLeft
        nodeLeft.parent = nodeParent

        # Fix the heights
        newNodeHeight = max(heightRight, heightLeftRight) + 1
        node.height = newNodeHeight
        nodeLeft.height = max(heightLeftLeft, newNodeHeight) + 1

        node.version.shrinking = False
        node.version.number += 1

        if (heightLeftRight - heightRight < -1 or heightLeftRight - heightRight > 1) or (
                (nodeLeftRight is None or heightRight == 0) and node.val is None):
            return node

        if (heightLeftLeft - newNodeHeight < -1 or heightLeftLeft - newNodeHeight > 1) or (heightLeftLeft == 0 and nodeLeft.val is None):
            return nodeLeft

        return self.__fixHeight(nodeParent)

    def __buildGraph(self, G, node, color=None):
        G.node(str(node.key), str(node.key) + " " + str(node.val), color= 'grey' if node.val is None else 'black')
        if color is not None:
            G.edge(str(node.parent.key), str(node.key), color=color)
        if node.left is not None:
            G = self.__buildGraph(G, node.left, color='blue')
        if node.right is not None:
            G = self.__buildGraph(G, node.right, color='red')
        return G

    def __prettyPrintTree(self, root):
        if root.right is None:
            print("Tree is empty!")
        else:
            G = Digraph(format='png')
            G = self.__buildGraph(G, root.right)
            display(Image(G.render()))
    
class Node(object):
    def __init__(self, key, val = None, parent=None):
        self.key = key  # comparable, assume int
        self.val = val
        self.height =  1

        # Pointers
        self.parent = parent  # None means this node is the root node
        self.left = None
        self.right = None
        
        # Concurrency Control
        self.version = self.Version()
        self.lock = threading.Lock()

    class Version(object):
        def __init__(self):
            self.unlinked = False
            self.growing = False
            self.shrinking = False
            self.number = 0

        def __eq__(self, other):
            if isinstance(other, self.__class__):
                return self.__dict__ == other.__dict__
            return False

        def __ne__(self, other):
            return not self.__eq__(other)
        
    def getChild(self, branch):
        """
        concurrent helper function, use branch=dkey-node.key
        branch<0: return left child, branch>0: return right child
        branch==0: return None
        """
        if branch < 0:
            return self.left
        elif branch > 0:
            return self.right
        else:
            return None

def strTree(droot):
    """
    perform a pretty print, stringified
    """
    stree = ""
    node = droot

    def DFSNode(dnode, dstree):
        if dnode is None:
            dstree += "·"
            return dstree
        else:
            dstree += dnode.val
        if dnode.height > 1:
            dstree += "("
            dstree = DFSNode(dnode.left, dstree)
            dstree += ","
            dstree = DFSNode(dnode.right, dstree)
            dstree += ")"
        return dstree

    stree = DFSNode(node, stree)
    return stree

def fakeConflict(self):
    if self.simulate == True:
        for i in range(random.randint(0,1000000)):
            pass