from graphviz import Digraph
from IPython.display import Image, display
import threading

UNLINK = -1
REBALANCE = -2
NOTHING = -3
class CC_RETRY(object):
    """
    Concurrent Control RETRY type
    usually happen in __getNode when child node version changes
    and retry can be performed from the same while loop again
    """
    pass

class CC_SPECIAL_RETRY(object):
    """
    Concurrent Control SPECIAL_RETRY type: read operation failed
    usually happen in __getNode when parent node version changes
    and retry should be performed from the caller function (with an updated dversion)
    """
    pass

class ConAVL(object):
    def __init__(self):
        self.root = None

    def get(self, dkey):
        return self.__getNode(self.root, dkey)

    def put(self, dkey, dval = None):
        self.__putNode(self.root, dkey, str(dkey) if dval is None else dval)
        self.__rebalance()

    def min(self):
        return self.__getMinNode(self.root)

    def max(self):
        return self.__getMaxNode(self.root)

    def remove(self, dkey):
        self.__removeNode(self.root, dkey)
        self.__rebalance()

    def print(self):
        self.__prettyPrintTree(self.root)

    def __rebalance(self):
        unbalanced = self.__balanceCheck(self.root)
        if unbalanced is not None:
            self.root = self.__getRoot(self.__autoRotate(unbalanced))
        
    def __str__(self):
        return strTree(self.root)

    def __getNode(self, droot, dkey):
        """
        if dkey presents, return the node,
        otherwise, return the future dkey's parent node
        """

        def __attemptGet(dnode, dkey, dversion, dbranch):
            while True:
                cnode = dnode.getChild(dbranch)
                if dnode.version != dversion:
                    return CC_SPECIAL_RETRY
                if cnode is None:
                    # no matching, should return the parent
                    return dnode

                nbranch = dkey - cnode.key
                if nbranch == 0:
                    # found matching, return cnode
                    return cnode

                cversion = cnode.version
                if cnode.version.shrinking or cnode.version.unlinked:
                    # wait
                    self.__waitUntilShrinkCompleted(cnode, cversion)
                    if dnode.version != dversion:
                        # should fall back to caller to gain a new dversion
                        return CC_SPECIAL_RETRY
                    # else RETRY: will fall back to while loop and retry on child node
                    continue
                elif cnode is not dnode.getChild(dbranch):
                    # cnode is no longer the correct cnode
                    if dnode.version != dversion:
                        return CC_SPECIAL_RETRY
                    # else RETRY
                    continue
                else:
                    if dnode.version != dversion:
                        return CC_SPECIAL_RETRY
                    # STILL VALID at this point !!!
                    vo = __attemptGet(cnode, dkey, cversion, dkey - cnode.key)
                    if vo != CC_SPECIAL_RETRY:
                        return vo
                    # else RETRY
                    continue

        # __getNode should test the root node first before entering the recursive attempt
        if droot is None or droot.key == dkey:
            return droot
        # enter the recursive attempt
        while True:
            cnode = droot.getChild(dkey - droot.key)
            if cnode is None:
                # no matching, should return the parent
                return droot

            cversion = cnode.version
            if cversion.shrinking or cversion.unlinked:
                # wait
                self.__waitUntilShrinkCompleted(cnode, cversion)
                # and will RETRY
                continue
            elif cnode is droot.getChild(dkey - droot.key):
                # still the same cnode, not changing
                vo = __attemptGet(cnode, dkey, cversion, dkey - cnode.key)
                if vo != CC_SPECIAL_RETRY:
                    return vo
            else:
                # RETRY
                continue

    def __putNode(self, droot, dkey, dval):
        """
        if dkey presents, perform update,
        otherwise perform insertion
        """

        tnode = self.__getNode(droot, dkey)
        if tnode is None:
            # init root
            self.root = Node(dkey, dval)
        elif tnode.key == dkey:
            # update
            tnode.val = dval
            # TODO: should we initialize a new Version?
        else:
            if dkey < tnode.key:
                tnode.left = Node(dkey, dval, tnode)
                self.__fixHeight(tnode.left)
            else:
                tnode.right = Node(dkey, dval, tnode)
                self.__fixHeight(tnode.right)

    def __waitUntilShrinkCompleted(self, dnode, dversion):
        """
        wait for lock to release, dversion is the version before waiting
        """
        if not dversion.shrinking:
            # version changed
            return
        # spin
        scnt = 100
        for i in range(scnt):
            if dnode.version != dversion:
                return

        dnode.lock.acquire()
        dnode.lock.release()

        assert dnode.version != dversion
        return

    def __getMinNode(self, droot):
        """
        return the node with minimum key, including droot
        """
        m = droot
        dnode = droot

        if droot is not None:
            while dnode.left != None:
                dnode = dnode.left
                if dnode.key < m.key:
                    m = dnode
        return m

    def __getMaxNode(self, droot):
        """
        return the node with maximum key, including droot
        """
        m = droot
        dnode = droot
        if droot is not None:
            while dnode.right != None:
                dnode = dnode.right
                if dnode.key > m.key:
                    m = dnode
        return m

    def __removeNode(self, droot, dkey):
        """
        if dkey exists, remove the corresponding node and return its parent,
        otherwise return None (means failure)
        """
        tnode = self.__getNode(droot, dkey)
        if tnode is not None:
            # found a node: just remove it
            if tnode.parent == None:
                # it's ROOT
                if tnode.left == None or tnode.right == None:
                    temp = tnode.left if tnode.left != None else tnode.right
                    if temp == None:
                        # 0 Children
                        self.root = None
                    else:
                        # 1 Child
                        temp.parent = None
                        self.root = temp
                else:
                    # 2 children
                    temp = self.__getMinNode(tnode.right)
                    tnode.left.parent = temp
                    temp.left = tnode.left
                    temp.parent = None
                    self.root = temp
            else:
                if tnode.height == 0:
                    # no children, simply remove itself
                    p = tnode.parent
                    if tnode.key == p.key:
                        # special case: in a recursive remove e.g. 8(4,7) -> 4(4,7) or 7(4,7)
                        if p.left != None and p.left.key == tnode.key:
                            p.left = None
                        else:
                            p.right = None
                    elif tnode.key < p.key:
                        p.left = None
                    else:
                        p.right = None
                    tnode.parent = None
                    self.__fixHeight(p)
                    return p
                elif tnode.left != None:
                    maxnode = self.__getMaxNode(tnode.left)
                    tnode.key = maxnode.key
                    tnode.val = maxnode.val
                    _ = self.__removeNode(tnode.left, maxnode.key)  # recursive, only once
                    # no need to __fixHeight
                    return tnode.parent
                else:
                    minnode = self.__getMinNode(tnode.right)
                    tnode.key = minnode.key
                    tnode.val = minnode.val
                    _ = self.__removeNode(tnode.right, minnode.key)  # recursive, only once
                    # no need to __fixHeight
                    return tnode.parent
        else:
            # cannot find a node: print WARNING
            print("WARNING: No matching node found, operation is invalidated.")
            return None

    def __fixHeightAndRebalance(self, dnode):
        """

        :param dnode:
        :return:
        """
        while dnode is not None and dnode.parent is not None:
            c = self.__nodeCondition(dnode)
            if c == NOTHING or dnode.version.unlinked == True:
                # node is fine, or node isn't repairable
                return

            if c is not UNLINK and c is not REBALANCE:
                with dnode.lock:
                    dnode = self.__fixHeight(dnode)

            else:
                nParent = dnode.parent
                with nParent.lock:
                    if nParent.version.unlinked == False and dnode.parent == nParent:
                        with dnode.lock:
                            dnode = self.__rebalanceNode(nParent, dnode)

    def __fixHeight(self, dnode):
        """
        Attempts to fix the height of a node. Returns the lowest damaged node that the current thread is responsible for or null if no damaged nodes are found.
        """

        c = self.__nodeCondition(dnode)
        if c == REBALANCE:
            # Need to rebalance
            pass
        elif c == UNLINK:
            # Need to unlink
            return dnode
        elif c == NOTHING:
            # This thread doesn't need to do anything
            return None
        else:
            # Fix height, return parent which will need fixing next
            dnode.height = c
            return dnode.parent

    def __nodeCondition(self, dnode):
        """
        Returns whether a node needs to be fixed (the correct height) or other status codes.
        """

        nL = dnode.left
        nR = dnode.right

        if (nL is None or nR is None) and dnode.val is None:
            # Need to unlink
            return UNLINK

        hN = dnode.height
        hL0 = 0 if nL is None else nL.height
        hR0 = 0 if nR is None else nR.height

        hNRepl = max(hL0, hR0) +1

        if hL0 - hR0 != 0:
            # Need to rebalance
            return REBALANCE

        # No action needed
        return hNRepl if hN != hNRepl else NOTHING

    def __rebalanceNode(self, nParent, dnode):
        nL = dnode.left #TODO: See some of the unshared stuff from snaoshot and see if it's necessary
        nR = dnode.right

        if (nL is None or nR is None) and dnode.val is None:
            if self.__attemptUnlink(nParent, dnode):
                # fix parent height
                return self.__fixHeight(nParent)
            return dnode
        hN = dnode.height
        hL0 = 0 if nL is None else nL.height
        hR0 = 0 if nR is None else nR.height

        hNRepl = max(hL0, hR0) +1

        if hL0 - hR0 > 1:
            return self.__rebalanceLeft(nParent, dnode, nR, hL0)
        elif hL0 - hR0 < 1:
            return self.__rebalanceRight(nParent, dnode, nL, hR0)
        elif hN == hNRepl:
            return None
        else:
            # fix height and fix the parent
            dnode.height = hNRepl
            return self.__fixHeight(nParent)

    def __rebalanceLeft(self, nParent, dnode, nR, hL0):
        with nR.lock:
            hR = nR.height
            if hL0 - hR <= -1:
                # retry
                return dnode
            else:
                nRL = nR.left #todo: see unshared again
                hRL0 = 0 if nRL is None else nRL.height
                hRR0 = 0 if nR.right is None else nR.right.height
                if hRR0 >= hRL0:
                    return self.__rotateLeft(nParent, dnode, hL0, nR, nRL, hRL0, hRR0)
                else:
                    with nRL.lock:
                        hRL = nRL.height
                        if hRR0 >= hRL:
                            return self.__rotateLeft(nParent, dnode, hL0, nR, nRL, hRL, hRR0)
                        else:
                            hRLR = 0 if nRL.right is None else nRL.right.height
                            if hRR0 - hRLR != 0 and not(hRR0 == 0 or hRLR == 0) and nR.val == None:
                                return self.__rotateLeftOverRight(nParent, dnode, hL0, nR, nRL, hRR0, hRLR)
                    return self.__rebalanceRight(dnode, nR, nRL, hRR0)

    def __rebalanceRight(self, nParent, dnode, nL, hR0):
        with nL.lock:
            hL = nL.height
            if hR0 - hL <= -1:
                #retry
                return dnode
            else:
                nLR = nL.right  # todo: see unshared again
                hLR0 = 0 if nLR is None else nLR.height
                hLL0 = 0 if nL.left is None else nL.left.height
                if hLL0 >= hLR0:
                    return self.__rotateRight(nParent, dnode, nL, hR0, hLL0, nLR, hLR0)
                else:
                    with nLR.lock:
                        hLR = nLR.height
                        if hLL0 >= hLR:
                            return self.__rotateRight(nParent, dnode, nL, hR0, hLL0, nLR, hLR)
                        else:
                            hLRL = 0 if nLR.left is None else nLR.left.height
                            if hLL0 - hLRL != 0 and not (hLL0 == 0 or hLRL == 0) and nL.val == None:
                                return self.__rotateRightOverLeft(nParent, dnode, nL, hR0, hLL0, nLR, hLRL)
                    return self.__rebalanceLeft(dnode, nL, nLR, hLL0)

    def __balanceCheck(self, droot):
        """
        should check after every modification
        return imbalanced node for rotation,
        otherwise return None
        """
        dnode = droot
        if droot is None:
            return None

        nl = dnode.left.height if dnode.left != None else -1
        nr = dnode.right.height if dnode.right != None else -1
        RR = None
        if abs(nl - nr) > 1:
            if nl * nr <= 0:
                # unbalanced node: direct return
                return dnode
            else:
                RR = dnode
        Rl = None
        Rr = None
        if dnode.left != None:
            Rl = self.__balanceCheck(dnode.left)
        if Rl != None:
            return Rl
        if dnode.right != None:
            Rr = self.__balanceCheck(dnode.right)
        if Rr != None:
            return Rr
        return RR

    def __rotateLL(self, dnode):
        """
        base type
        rotate LL type, return root node of subtree
        """
        k2 = dnode
        k1 = dnode.left
        y = k1.right
        if k2.parent != None:
            p = k2.parent
            if p.left != None and p.left.key == k2.key:
                p.left = k1
            else:
                p.right = k1
            k1.parent = p
        else:
            k1.parent = None

        k2.left = y
        if y != None:
            y.parent = k2

        k1.right = k2
        k2.parent = k1

        # fix heights here
        k2.height = max(
            k2.left.height if k2.left != None else -1,
            k2.right.height if k2.right != None else -1
        ) + 1
        k1.height = max(
            k1.left.height if k1.left != None else -1,
            k1.right.height if k1.right != None else -1
        ) + 1

        return k1

    def __rotateLR(self, dnode):
        """
        complex type
        rotate LR type, return root node of subtree
        """
        k3 = dnode
        k1 = k3.left

        # first do an RR on k1
        self.__rotateRR(k1)
        # then do an LL on k3, no need to fix height
        return self.__rotateLL(k3)

    def __rotateRL(self, dnode):
        """
        complex type
        rotate RL type, return root node of subtree
        """
        k1 = dnode
        k3 = k1.right
        k2 = k3.left

        # first do an LL on k3
        k2 = self.__rotateLL(k3)
        # then do an RR on k1
        k2 = self.__rotateRR(k1)
        # no need to fix height
        return k2

    def __rotateRR(self, dnode):
        """
        base type
        rotate RR type, return root node of subtree
        """
        k1 = dnode
        k2 = dnode.right
        y = k2.left
        if k1.parent != None:
            p = k1.parent
            if p.left != None and p.left.key == k1.key:
                p.left = k2
            else:
                p.right = k2
            k2.parent = p
        else:
            k2.parent = None

        k1.right = y
        if y != None:
            y.parent = k1

        k2.left = k1
        k1.parent = k2

        # fix heights here
        k1.height = max(
            k1.left.height if k1.left != None else -1,
            k1.right.height if k1.right != None else -1
        ) + 1
        k2.height = max(
            k2.left.height if k2.left != None else -1,
            k2.right.height if k2.right != None else -1
        ) + 1

        return k2

    def __autoRotate(self, dnode):
        """
        check and determine the type of rotation, and do the rotation
        """
        nl = dnode.left.height if dnode.left != None else -1
        nr = dnode.right.height if dnode.right != None else -1

        if nl < nr:
            # R*, nr>=0 must hold
            ddnode = dnode.right
            nnl = ddnode.left.height if ddnode.left != None else -1
            nnr = ddnode.right.height if ddnode.right != None else -1
            if nnl < nnr:
                # print("RR")
                return self.__rotateRR(dnode)
            else:
                # print("RL")
                return self.__rotateRL(dnode)
        else:
            # L*, nl>=0 must hold
            ddnode = dnode.left
            nnl = ddnode.left.height if ddnode.left != None else -1
            nnr = ddnode.right.height if ddnode.right != None else -1
            if nnl < nnr:
                # print("LR")
                return self.__rotateLR(dnode)
            else:
                # print("LL")
                return self.__rotateLL(dnode)

    def __getRoot(self, dnode):
        """
        trace back to the actual top ROOT node
        """
        if dnode != None:
            ddnode = dnode
            while ddnode.parent != None:
                ddnode = ddnode.parent
            return ddnode

    def __buildGraph(self, G, node, color=None):
        G.node(str(node.key), str(node.key))
        if color is not None:
            G.edge(str(node.parent.key), str(node.key), color=color)
        if node.left is not None:
            G = self.__buildGraph(G, node.left, color='blue')
        if node.right is not None:
            G = self.__buildGraph(G, node.right, color='red')
        return G

    def __prettyPrintTree(self, root):
        if root == None:
            print("Tree is empty!")
        else:
            G = Digraph(format='png')
            G = self.__buildGraph(G, root)
            display(Image(G.render()))
    
class Node(object):
    def __init__(self, dkey, dval = None, parent=None):
        self.key = dkey  # comparable, assume int
        self.val = dval  # any type, None means this node is conceptually not present
        self.height =  0

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
            self.gcnt = 0  # growing count incr
            self.shrinking = False
            self.scnt = 0  # shrinking count incr

        def __eq__(self, other):
            if isinstance(other, self.__class__):
                return self.__dict__ == other.__dict__
            return False

        def __ne__(self, other):
            return not self.__eq__(other)
        
    def getChild(self, branch):
        """
        concurrent helper function, use branch=dkey-dnode.key
        branch<0: return left child, branch>0: return right child
        branch==0: return None
        """
        if branch<0:
            return self.left
        elif branch>0:
            return self.right
        else:
            return None

def strTree(droot):
    """
    perform a pretty print, stringified
    """
    stree = ""
    dnode = droot

    def DFSNode(ddnode, dstree):
        if ddnode == None:
            dstree += "·"
            return dstree
        else:
            dstree += ddnode.val
        if ddnode.height > 0:
            dstree += "("
            dstree = DFSNode(ddnode.left, dstree)
            dstree += ","
            dstree = DFSNode(ddnode.right, dstree)
            dstree += ")"
        return dstree

    stree = DFSNode(dnode, stree)
    return stree
