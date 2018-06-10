from graphviz import Digraph
from IPython.display import Image, display
import threading

UNLINK = -1
REBALANCE = -2
NOTHING = -3

UPDATE_ALWAYS = 0
UPDATE_IF_ABSENT = 1
UPDATE_IF_PRESENT = 2
UPDATE_IF_EQ = 3

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

class CC_UNLINKED_OVL(object):
    pass

class ConAVL(object):
    def __init__(self):
        self.root = None

    def get(self, dkey):
        return self.__getNode(self.root, dkey)

    def put(self, dkey, dval = None):
        self.__putNode(dkey, UPDATE_ALWAYS, None, str(dkey) if dval is None else dval, self.root) #TODO: what the hell is this holder thing?

    def min(self):
        return self.__getMinNode(self.root)

    def max(self):
        return self.__getMaxNode(self.root)

    def remove(self, dkey):
        self.__removeNode(self.root, dkey)

    def print(self):
        self.__prettyPrintTree(self.root)
        
    def __str__(self):
        return strTree(self.root)

    def __getNode(self, droot, dkey):
        """
        if dkey presents, return the value,
        otherwise, return None
        """

        def attemptGet(self, key, node, dirToC, nodeOVL):
            while True:
                cnode = node.getChild(dirToC)
                if cnode is None:
                    if node.version != nodeOVL:
                        return CC_SPECIAL_RETRY
                    return None
                else:
                    childCmp = key - cnode.key
                    if childCmp == 0:
                        return cnode.value
                    cversion = cnode.version
                    if cversion.shrinking or cversion.unlinked:
                        self.__waitUntilShrinkCompleted(cnode, cversion)
                        if node.version != nodeOVL:
                            return CC_SPECIAL_RETRY
                        # else: RETRY
                    elif cnode is not node.getChild(dirToC):
                        if node.version != nodeOVL:
                            return CC_SPECIAL_RETRY
                        # else: RETRY
                    else:
                        if node.version != nodeOVL:
                            return CC_SPECIAL_RETRY
                        vo = attemptGet(self, key, cnode, childCmp, cversion)
                        if vo != CC_SPECIAL_RETRY:
                            return vo
                        # else: RETRY

                    
        while True:
            cnode = droot.getChild(dkey - droot.key)
            if cnode is None:
                return None
            else:
                cmp = dkey - cnode.key
                if cmp == 0:
                    return
                cversion = cnode.version
                if cversion.shrinking or cversion.unlinked:
                    self.__waitUntilShrinkCompleted(cnode, cversion)
                    # and then RETRY
                    continue
                elif cnode is droot.getChild(dkey - droot.key):
                    # still the same cnode, not changing
                    vo = attemptGet(self, dkey, cnode, dkey-cnode.key, cversion)
                    if vo != CC_SPECIAL_RETRY:
                        return vo
                    # else: RETRY
                else:
                    # RETRY
                    continue

    # equal to "updateUnderRoot"
    # original -> renamed
    # k -> /: k is a comparable version of key
    ## right -> cnode: child node
    ## ovl -> cversion: child version
    def __putNode(self, key, func, expected, newValue, holder):
        """
        TO-UPDATE
        """
        
        # helper function of __putNode
        def attemptInsertIntoEmpty(key, vOpt, holder):
            result = None # should be set to True or False when return
            with holder.lock:
                cnode = holder.getChild(key - holder.key) # lock before get, looks good
                if cnode is None:
                    if key<holder.key:
                        holder.left = Node(key, vOpt, holder)
                    else:
                        # key>holder.key
                        holder.right = Node(key, vOpt, holder)
                    result = True
                    holder.height = 1
                else:
                    result = False
            return result
        
        # helper function of __putNode
        def attemptUpdate(key, func, expected, newValue, parent, node, nodeOVL):
            # == ignore the assert
            cmp = key - node.key
            if cmp == 0:
                return attemptNodeUpdate(func, expected, newValue, parent, node)
            while True:
                child = node.getChild(cmp)
                if node.version != nodeOVL:
                    return CC_SPECIAL_RETRY
                
                if child is None:
                    # key is not present
                    if newValue is None:
                        # removal is requested
                        return None
                    else:
                        # update will be an insert
                        success = None
                        # == ignore damaged 
                        with node.lock:
                            if node.version != nodeOVL:
                                return CC_SPECIAL_RETRY
                            if node.getChild(cmp) is not None:
                                # lost a race with a concurrent insert
                                # must retry in the outer loop
                                success = False
                                damaged = None
                                # == ignore damage
                                # will RETRY
                            else:
                                if not self.__shouldUpdate(func, None, expected):
                                    return False if func == UPDATE_IF_EQ else None
                                if key < holder.key:
                                    holder.left = Node(key, newValue, holder)
                                    success = True
                                else:
                                    # key>holder.key
                                    holder.right = Node(key, newValue, holder)
                                    success = True
                                damaged = self.__fixHeight(holder)

                                # == ignore damage
                        if success:
                            self.__fixHeightAndRebalance(damaged)
                            return True if func == UPDATE_IF_EQ else None
                        # else: RETRY
                else:
                    childOVL = child.version
                    if childOVL.shrinking or childOVL.unlinked:
                        self.__waitUntilShrinkCompleted(child, childOVL)
                        # and then RETRY
                    elif child is not node.getChild(cmp):
                        continue # which is RETRY
                    else:
                        if node.version != nodeOVL:
                            return CC_SPECIAL_RETRY
                        vo = attemptUpdate(key, func, expected, newValue, node, child, childOVL)
                        if vo != CC_SPECIAL_RETRY:
                            return vo
                        # else: RETRY
           
        # helper function of __putNode
        def attemptNodeUpdate(func, expected, newValue, parent, node):
            if newValue is None:
                # removal
                if node.value is None:
                    # already removed, nothing to do
                    return None
            
            if newValue is None and (node.left is None or node.right is None):
                # potential unlink, get ready by locking the parent
                # == ignore prev
                # == ignore damage
                with parent.lock:
                    if parent.version.unlinked or node.parent != parent:
                        return CC_SPECIAL_RETRY
                    with node.lock:
                        prev = node.value
                        if not self.__shouldUpdate(func, prev, expected):
                            return False if func == UPDATE_IF_EQ else prev
                        if prev is None:
                            return True if func == UPDATE_IF_EQ else prev
                        if not self.__attemptUnlink(parent,node):
                            return CC_SPECIAL_RETRY
                    # == ignore damage
                    damaged = self.__fixHeight(parent)
                self.__fixHeightAndRebalance(damaged)
                return True if func == UPDATE_IF_EQ else prev
            else:
                with node.lock:
                    if node.version.unlinked:
                        return CC_SPECIAL_RETRY
                    prev = node.value
                    if not self.__shouldUpdate(func, prev, expected):
                        return False if func == UPDATE_IF_EQ else prev
                    # retry if we now detect that unlink is possible
                    if newValue is None and (node.left is None or node.right is None):
                        return CC_SPECIAL_RETRY
                    node.value = newValue
                return True if func == UPDATE_IF_EQ else prev
            
        # =============================================================================
        while True:
            # choose the proper child node
            cnode = holder.getChild(key - holder.key)
            if cnode is None:
                # key is not present
                if not self.__shouldUpdate(func, None, expected):
                    # None means the value does not exist
                    return False if func == UPDATE_IF_EQ else None
                if newValue is None or attemptInsertIntoEmpty(key, newValue, holder):
                    return True if func == UPDATE_IF_EQ else None
                # else: RETRY
            else:
                cversion = cnode.version
                if cversion.shrinking or cversion.unlinked:
                    self.__waitUntilShrinkCompleted(cnode, cversion)
                    # and then RETRY
                elif cnode is holder.getChild(key - holder.key):
                    vo = attemptUpdate(key, func, expected, newValue, holder, cnode, cversion)
                    if vo != CC_SPECIAL_RETRY:
                        return vo
                # else: RETRY
        
    def __attemptUnlink(parent, node):
        # == ignore assert
        parentL = parent.left
        parentR = parent.right
        if parentL != node and parentR != node:
            # node is no longer a child of parent
            return False

        # == ignore assert
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

        node.version = CC_UNLINKED_OVL
        node.value = None

        return True
    
    def __shouldUpdate(self, func, prev, expected):
        """
        prev: value type
        expexted: value type
        """
        if func == UPDATE_ALWAYS:
            return True
        elif func == UPDATE_IF_ABSENT:
            # None here means the value does not exist, compared to CC_NULL which means to be removed
            return prev is None
        elif func == UPDATE_IF_PRESENT:
            # None here means the value does not exist, compared to CC_NULL which means to be removed
            return prev is not None
        else: # UPDATE_IF_EQ
            # == ignore assert
            if prev is None:
                return False
            # == AllowNullValues is False, ignore related codes
            return prev == expected
    
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
            while dnode.left is not None:
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
            while dnode.right is not None:
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
            if tnode.parent is None:
                # it's ROOT
                if tnode.left is None or tnode.right is None:
                    temp = tnode.left if tnode.left is not None else tnode.right
                    if temp is None:
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
                        if p.left is not None and p.left.key == tnode.key:
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
                elif tnode.left is not None:
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
                            if hRR0 - hRLR != 0 and not(hRR0 == 0 or hRLR == 0) and nR.val is None:
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
                    return self.__rotateRight(nParent, dnode, hL, hR0, nLR, hLR0, hLL0)
                else:
                    with nLR.lock:
                        hLR = nLR.height
                        if hLL0 >= hLR:
                            return self.__rotateRight(nParent, dnode, hL, hR0, nLR, hLR, hLL0)
                        else:
                            hLRL = 0 if nLR.left is None else nLR.left.height
                            if hLL0 - hLRL != 0 and not (hLL0 == 0 or hLRL == 0) and nL.val is None:
                                return self.__rotateRightOverLeft(nParent, dnode, hR0, nL, nLR, hLL0, hLRL)
                    return self.__rebalanceLeft(dnode, nL, nLR, hLL0)

    def __rotateLeftOverRight(self, nParent, dnode, hL, nR, nRL, hRR, hRLR):

        nPL = nParent.left
        nRLL = nRL.left
        nRLR = nRL.right
        hRLL = 0 if nRLL is None else nRLL.height

        dnode.version.shrinking = True
        nR.version.shrinking = True

        # Fix all the pointers
        dnode.right = nRLL
        if nRLL is not None:
            nRLL.parent = dnode

        nR.left = nRLR
        if nRLR is not None:
            nRLR.parent = nR

        nRL.right = nR
        nR.parent = nRL
        nRL.left = dnode
        dnode.parent = nRL

        if nPL != dnode:
            nParent.right = nRL
        else:
            nParent.left = nRL
        nRL.parent = nParent

        # Fix all the heights
        hNRepl = max(hRLL, hL) +1
        dnode.height = hNRepl
        hRRepl = max(hRR, hRLR) +1
        nR.height = hRRepl
        nRL.height = max(hNRepl, hRRepl) + 1

        dnode.version.shrinking = False
        nR.version.shrinking = False

        assert abs(hRR-hRLR) <= 1

        if (hRLL - hL < -1 or hRLL - hL > 1) or ((nRLL is None or hL == 0) and dnode.val is None):
            return dnode

        if (hRRepl - hNRepl < -1 or hRRepl - hNRepl > 1) :
            return nRL
        return self.__fixHeight(nParent)

    def __rotateRightOverLeft(self, nParent, dnode, hR, nL, nLR, hLL, hLRL):

        nPL = nParent.left
        nLRL = nLR.left
        nLRR = nLR.right
        hLRR = 0 if nLRR is None else nLRR.height

        dnode.version.shrinking = True
        nL.version.shrinking = True

        # Fix all the pointers

        dnode.left = nLRR
        if nLRR is not None:
            nLRR.parent = dnode

        nL.right = nLRL
        if nLRL is not None:
            nLRL.parent = nL


        nLR.right = nL
        nL.parent = nLR
        nLR.right = dnode
        dnode.parent = nLR

        if nPL != dnode:
            nParent.right = nLR
        else:
            nParent.left = nLR
        nLR.parent = nParent

        # Fix all the heights
        hNRepl = max(hLRR, hR) + 1
        dnode.height = hNRepl
        hLRepl = max(hLL, hLRL) + 1
        nL.height = hLRepl
        nLR.height = max(hNRepl, hLRepl) + 1

        dnode.version.shrinking = False
        nL.version.shrinking = False

        assert abs(hLL - hLRL) <= 1
        assert not ((hLL == 0 or nLRL is None) and nL.val is None)

        if (hLRR - hR < -1 or hLRR - hR > 1) or ((nLRR is None or hR == 0) and dnode.val is None):
            return dnode

        if (hLRepl - hNRepl < -1 or hLRepl - hNRepl > 1) :
            return nLR
        return self.__fixHeight(nParent)

    def __rotateLeft(self, nParent, dnode, hL, nR, nRL, hRL, hRR):
        nPL = nParent.left # sibling or itself

        dnode.version.shrinking = True

        # Fix all the pointers
        dnode.right = nRL
        if nRL is not None:
            nRL.parent = dnode

        nR.left = dnode
        dnode.parent = nR

        if nPL is dnode:
            nParent.left = nR
        else:
            nParent.right = nR
        nR.parent = nParent

        # Fix the heights
        hNRepl = max(hL, hRL) + 1
        dnode.height = hNRepl
        nR.height = max(hRR, hNRepl) + 1

        dnode.version.shrinking = False

        if (hRL - hL < -1 or hRL - hL > 1) or ((nRL is None or hL == 0) and dnode.val is None): #TODO: Why do we check for val is None?
            return dnode

        if (hRR - hNRepl< -1 or hRR - hNRepl > 1) or (hRR == 0 and nR.val is None):
            return nR

        return self.__fixHeight(nParent)

    def __rotateRight(self, nParent, dnode, hR, nL, nLR, hLR, hLL):
        nPL = nParent.left  # sibling or itself

        dnode.version.shrinking = True

        # Fix all the pointers
        dnode.left = nLR
        if nLR is not None:
            nLR.parent = dnode

        nL.right = dnode
        dnode.parent = nL

        if nPL is dnode:
            nParent.left = nL
        else:
            nParent.right = nL
        nL.parent = nParent

        # Fix the heights
        hNRepl = max(hR, hLR) + 1
        dnode.height = hNRepl
        nL.height = max(hLL, hNRepl) + 1

        dnode.version.shrinking = False

        if (hLR - hR < -1 or hLR - hR > 1) or (
                (nLR is None or hR == 0) and dnode.val is None):  # TODO: Why do we check for val is None?
            return dnode

        if (hLL - hNRepl < -1 or hLL - hNRepl > 1) or (hLL == 0 and nL.val is None):
            return nL

        return self.__fixHeight(nParent)

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
        if root is None:
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
        if ddnode is None:
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
