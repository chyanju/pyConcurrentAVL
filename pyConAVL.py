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

    def __init__(self):
        """
        Initializes the ConAVL object. The root is set as an empty node. root.right will point to the actual root.
        """
        self.root = Node(None)

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
                                    fakeConflict()
                                    node.left = Node(key, val=newValue, parent=node)
                                else:
                                    # key>root.key
                                    fakeConflict()
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
            fakeConflict()
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
                    fakeConflict()
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

        node.version = CC_UNLINKED
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
                nParent = node.parent
                with nParent.lock:
                    if nParent.version.unlinked == False and node.parent == nParent:
                        with node.lock:
                            node = self.__rebalanceNode(nParent, node)

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

    def __rebalanceNode(self, nParent, node):
        nL = node.left #TODO: See some of the unshared stuff from snaoshot and see if it's necessary
        nR = node.right
        if (nL is None or nR is None) and node.val is None:
            if self.__attemptUnlink(nParent, node):
                # fix parent height
                return self.__fixHeight(nParent)
            return node
        hN = node.height
        hL0 = 0 if nL is None else nL.height
        hR0 = 0 if nR is None else nR.height

        hNRepl = max(hL0, hR0) +1
        if hL0 - hR0 < -1:
            return self.__rebalanceLeft(nParent, node, nR, hL0)
        elif hL0 - hR0 > 1:
            return self.__rebalanceRight(nParent, node, nL, hR0)
        elif hN == hNRepl:
            return None
        else:
            # fix height and fix the parent
            node.height = hNRepl
            return self.__fixHeight(nParent)

    def __rebalanceLeft(self, nParent, node, nR, hL0):
        with nR.lock:
            hR = nR.height
            if hL0 - hR >= -1:
                # retry
                return node
            else:
                nRL = nR.left #todo: see unshared again
                hRL0 = 0 if nRL is None else nRL.height
                hRR0 = 0 if nR.right is None else nR.right.height
                if hRR0 >= hRL0:
                    return self.__rotateLeft(nParent, node, hL0, nR, nRL, hRL0, hRR0)
                else:
                    with nRL.lock:
                        hRL = nRL.height
                        if hRR0 >= hRL:
                            return self.__rotateLeft(nParent, node, hL0, nR, nRL, hRL, hRR0)
                        else:
                            hRLR = 0 if nRL.right is None else nRL.right.height
                            if (hRR0 - hRLR >= -1 and  hRR0 - hRLR <= 1)and not ((hRR0 == 0 or hRLR == 0) and nR.val is None):
                                return self.__rotateLeftOverRight(nParent, node, hL0, nR, nRL, hRR0, hRLR)
                    return self.__rebalanceRight(node, nR, nRL, hRR0)

    def __rebalanceRight(self, nParent, node, nL, hR0):
        with nL.lock:
            hL = nL.height
            if hL - hR0 <= 1:
                #retry
                return node
            else:
                nLR = nL.right  # todo: see unshared again
                hLR0 = 0 if nLR is None else nLR.height
                hLL0 = 0 if nL.left is None else nL.left.height
                if hLL0 >= hLR0:
                    return self.__rotateRight(nParent, node, hR0, nL,  nLR, hLR0, hLL0)
                else:
                    with nLR.lock:
                        hLR = nLR.height
                        if hLL0 >= hLR:
                            return self.__rotateRight(nParent, node, hR0, nL, nLR, hLR, hLL0)
                        else:
                            hLRL = 0 if nLR.left is None else nLR.left.height
                            if (hLL0 - hLRL >= -1 and hLL0 - hLRL <= 1) and not ((hLL0 == 0 or hLRL == 0) and nL.val is None):
                                return self.__rotateRightOverLeft(nParent, node, hR0, nL, nLR, hLL0, hLRL)
                    return self.__rebalanceLeft(node, nL, nLR, hLL0)

    def __rotateLeftOverRight(self, nParent, node, hL, nR, nRL, hRR, hRLR):

        nPL = nParent.left
        nRLL = nRL.left
        nRLR = nRL.right
        hRLL = 0 if nRLL is None else nRLL.height

        node.version.shrinking = True
        nR.version.shrinking = True

        # Fix all the pointers
        node.right = nRLL
        if nRLL is not None:
            nRLL.parent = node

        nR.left = nRLR
        if nRLR is not None:
            nRLR.parent = nR

        nRL.right = nR
        nR.parent = nRL
        nRL.left = node
        node.parent = nRL

        if nPL != node:
            nParent.right = nRL
        else:
            nParent.left = nRL
        nRL.parent = nParent

        # Fix all the heights
        hNRepl = max(hRLL, hL) +1
        node.height = hNRepl
        hRRepl = max(hRR, hRLR) +1
        nR.height = hRRepl
        nRL.height = max(hNRepl, hRRepl) + 1

        node.version.shrinking = False
        nR.version.shrinking = False

        assert abs(hRR-hRLR) <= 1

        if (hRLL - hL < -1 or hRLL - hL > 1) or ((nRLL is None or hL == 0) and node.val is None):
            return node

        if (hRRepl - hNRepl < -1 or hRRepl - hNRepl > 1) :
            return nRL
        return self.__fixHeight(nParent)

    def __rotateRightOverLeft(self, nParent, node, hR, nL, nLR, hLL, hLRL):

        nPL = nParent.left
        nLRL = nLR.left
        nLRR = nLR.right
        hLRR = 0 if nLRR is None else nLRR.height

        node.version.shrinking = True
        nL.version.shrinking = True

        # Fix all the pointers

        node.left = nLRR
        if nLRR is not None:
            nLRR.parent = node

        nL.right = nLRL
        if nLRL is not None:
            nLRL.parent = nL

        nLR.left = nL
        nL.parent = nLR
        nLR.right = node
        node.parent = nLR

        if nPL != node:
            nParent.right = nLR
        else:
            nParent.left = nLR
        nLR.parent = nParent

        # Fix all the heights
        hNRepl = max(hLRR, hR) + 1
        node.height = hNRepl
        hLRepl = max(hLL, hLRL) + 1
        nL.height = hLRepl
        nLR.height = max(hNRepl, hLRepl) + 1

        node.version.shrinking = False
        nL.version.shrinking = False

        assert abs(hLL - hLRL) <= 1
        assert not ((hLL == 0 or nLRL is None) and nL.val is None)

        if (hLRR - hR < -1 or hLRR - hR > 1) or ((nLRR is None or hR == 0) and node.val is None):
            return node

        if (hLRepl - hNRepl < -1 or hLRepl - hNRepl > 1) :
            return nLR
        return self.__fixHeight(nParent)

    def __rotateLeft(self, nParent, node, hL, nR, nRL, hRL, hRR):
        nPL = nParent.left # sibling or itself

        node.version.shrinking = True

        # Fix all the pointers
        node.right = nRL
        if nRL is not None:
            nRL.parent = node

        nR.left = node
        node.parent = nR

        if nPL is node:
            nParent.left = nR
        else:
            nParent.right = nR
        nR.parent = nParent

        # Fix the heights
        hNRepl = max(hL, hRL) + 1
        node.height = hNRepl
        nR.height = max(hRR, hNRepl) + 1

        node.version.shrinking = False

        if (hRL - hL < -1 or hRL - hL > 1) or ((nRL is None or hL == 0) and node.val is None): #TODO: Why do we check for val is None?
            return node

        if (hRR - hNRepl< -1 or hRR - hNRepl > 1) or (hRR == 0 and nR.val is None):
            return nR

        return self.__fixHeight(nParent)

    def __rotateRight(self, nParent, node, hR, nL, nLR, hLR, hLL):
        nPL = nParent.left  # sibling or itself

        node.version.shrinking = True

        # Fix all the pointers
        node.left = nLR
        if nLR is not None:
            nLR.parent = node

        nL.right = node
        node.parent = nL

        if nPL is node:
            nParent.left = nL
        else:
            nParent.right = nL
        nL.parent = nParent

        # Fix the heights
        hNRepl = max(hR, hLR) + 1
        node.height = hNRepl
        nL.height = max(hLL, hNRepl) + 1

        node.version.shrinking = False

        if (hLR - hR < -1 or hLR - hR > 1) or (
                (nLR is None or hR == 0) and node.val is None):  # TODO: Why do we check for val is None?
            return node

        if (hLL - hNRepl < -1 or hLL - hNRepl > 1) or (hLL == 0 and nL.val is None):
            return nL

        return self.__fixHeight(nParent)

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

def fakeConflict():
    for i in range(random.randint(0,1000000)):
        pass