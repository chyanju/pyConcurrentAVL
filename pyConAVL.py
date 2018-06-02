from graphviz import Digraph
from IPython.display import Image, display


class ConAVL(object):
    def __init__(self):
        self.root = Node()

    def get(self, dkey):
        return getNode(self.root, dkey)

    def put(self, dkey, dval = None):
        putNode(self.root, dkey, str(dkey) if dval is None else dval)
        unbalanced = balanceCheck(self.root)
        if unbalanced is not None:
            self.root = getRoot(autoRotate(unbalanced))

    def min(self):
        return getMinNode(self.root)

    def max(self):
        return getMaxNode(self.root)

    def remove(self, dkey):
        removeNode(self.root, dkey)
        unbalanced = balanceCheck(self.root)
        if unbalanced is not None:
            self.root = getRoot(autoRotate(unbalanced))

    def print(self):
        prettyPrintTree(self.root)
        
    def __str__(self):
        return strTree(self.root)

class Node(object):
    def __init__(self, dkey = None, dval = None):
        self.key = dkey  # comparable, assume int
        self.val = dval  # any type, None means this node is conceptually not present

        self.height = 0
        if dkey is None:
            self.height = -1 # -1 if the node doesn't exist (a fake node with key: None)

        # Nodes
        self.parent = None  # None means this node is the root node
        self.left = None
        self.right = None

    def copy(self, node):
        self.key = node.key
        self.val = node.val

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

def getNode(droot, dkey):
    """
    if dkey presents, return the node,
    otherwise, return the future dkey's parent node
    """
    dnode = droot
    while True:
        if dnode.key == dkey or dnode.height == -1:
            return dnode
        else:
            if dkey < dnode.key:
                if dnode.left == None:
                    return dnode
                else:
                    dnode = dnode.left
            else:
                if dnode.right == None:
                    return dnode
                else:
                    dnode = dnode.right

def putNode(droot, dkey, dval):
    """
    if dkey presents, perform update,
    otherwise perform insertion
    """

    tnode = getNode(droot, dkey)
    if tnode.height==-1:
        # init root
        tnode.key = dkey
        tnode.val = dval
        tnode.height = 0
    elif tnode.key == dkey:
        # update
        tnode.val = dval
    else:
        # insert
        if tnode.key is None:
            tnode.key = dkey
            tnode.val = dval
        elif dkey < tnode.key:
            tnode.left = Node(dkey, dval)
            tnode.left.parent = tnode
            fixHeight(tnode.left)
        else:
            tnode.right = Node(dkey, dval)
            tnode.right.parent = tnode
            fixHeight(tnode.right)

def getMinNode(droot):
    """
    return the node with minimum key, including droot
    """
    m = droot
    dnode = droot
    while dnode.left != None:
        dnode = dnode.left
        if dnode.key < m.key:
            m = dnode
    return m

def getMaxNode(droot):
    """
    return the node with maximum key, including droot
    """
    m = droot
    dnode = droot
    while dnode.right != None:
        dnode = dnode.right
        if dnode.key > m.key:
            m = dnode
    return m

def removeNode(droot, dkey):
    """
    if dkey exists, remove the corresponding node and return its parent,
    otherwise return None (means failure)
    """
    tnode = getNode(droot, dkey)
    if tnode.key == dkey:
        # found a node: just remove it
        if tnode.parent == None:
            # it's ROOT
            if tnode.left == None or tnode.right == None:
                temp = tnode.left if tnode.left != None else tnode.right
                if temp == None:
                    # 0 Children
                    tnode.copy(Node())
                else:
                    # 1 Child
                    tnode.copy(temp)
                    removeNode(temp, temp.key)
            else:
                # 2 children
                temp = getMinNode(tnode.right)
                tnode.copy(temp)
                removeNode(tnode.right, tnode.key)
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
                fixHeight(p)
                return p
            elif tnode.left != None:
                maxnode = getMaxNode(tnode.left)
                tnode.key = maxnode.key
                tnode.val = maxnode.val
                _ = removeNode(tnode.left, maxnode.key)  # recursive, only once
                # no need to fixHeight
                return tnode.parent
            else:
                minnode = getMinNode(tnode.right)
                tnode.key = minnode.key
                tnode.val = minnode.val
                _ = removeNode(tnode.right, minnode.key)  # recursive, only once
                # no need to fixHeight
                return tnode.parent
    else:
        # cannot find a node: print WARNING
        print("WARNING: No matching node found, operation is invalidated.")
        return None

def fixHeight(dnode):
    """
    fix the height properties from dnode back to ROOT
    used in remove method
    """
    if dnode.left == None and dnode.right == None:
        dnode.height = 0
    else:
        dnode.height = max(
            dnode.left.height if dnode.left != None else -1,
            dnode.right.height if dnode.right != None else -1
        ) + 1
    ddnode = dnode
    while ddnode.parent != None:
        ddnode = ddnode.parent
        if ddnode.left == None and ddnode.right == None:
            ddnode.height = 0
        else:
            ddnode.height = max(
                ddnode.left.height if ddnode.left != None else -1,
                ddnode.right.height if ddnode.right != None else -1
            ) + 1

def balanceCheck(droot):
    """
    should check after every modification
    return imbalanced node for rotation,
    otherwise return None
    """
    dnode = droot
    nl = dnode.left.height if dnode.left!=None else -1
    nr = dnode.right.height if dnode.right!=None else -1
    RR = None
    if abs(nl-nr)>1:
        if nl*nr<=0:
            # unbalanced node: direct return
            return dnode
        else:
            RR = dnode
    Rl = None
    Rr = None
    if dnode.left!=None:
        Rl = balanceCheck(dnode.left)
    if Rl!=None:
        return Rl
    if dnode.right!=None:
        Rr = balanceCheck(dnode.right)
    if Rr!=None:
        return Rr
    return RR

def rotateLL(dnode):
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

def rotateLR(dnode):
    """
    complex type
    rotate LR type, return root node of subtree
    """
    k3 = dnode
    k1 = k3.left
    k2 = k1.right

    # first do an RR on k1
    k2 = rotateRR(k1)
    # then do an LL on k3
    k2 = rotateLL(k3)
    # no need to fix height
    return k2

def rotateRL(dnode):
    """
    complex type
    rotate RL type, return root node of subtree
    """
    k1 = dnode
    k3 = k1.right
    k2 = k3.left

    # first do an LL on k3
    k2 = rotateLL(k3)
    # then do an RR on k1
    k2 = rotateRR(k1)
    # no need to fix height
    return k2

def rotateRR(dnode):
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

def autoRotate(dnode):
    """
    check and determine the type of rotation, and do the rotation
    """
    nl = dnode.left.height if dnode.left!=None else -1
    nr = dnode.right.height if dnode.right!=None else -1

    if nl < nr:
        # R*, nr>=0 must hold
        ddnode = dnode.right
        nnl = ddnode.left.height if ddnode.left!=None else -1
        nnr = ddnode.right.height if ddnode.right!=None else -1
        if nnl < nnr:
            #print("RR")
            return rotateRR(dnode)
        else:
            #print("RL")
            return rotateRL(dnode)
    else:
        # L*, nl>=0 must hold
        ddnode = dnode.left
        nnl = ddnode.left.height if ddnode.left!=None else -1
        nnr = ddnode.right.height if ddnode.right!=None else -1
        if nnl < nnr:
            #print("LR")
            return rotateLR(dnode)
        else:
            #print("LL")
            return rotateLL(dnode)

def getRoot(dnode):
    """
    trace back to the actual top ROOT node
    """
    if dnode != None:
        ddnode = dnode
        while ddnode.parent != None:
            ddnode = ddnode.parent
        return ddnode

def buildGraph(G, node, color=None):
    G.node(str(node.key), str(node.key))
    if color is not None:
        G.edge(str(node.parent.key), str(node.key), color=color)
    if node.left is not None:
        G = buildGraph(G, node.left, color='blue')
    if node.right is not None:
        G = buildGraph(G, node.right, color='red')
    return G

def prettyPrintTree(root):
    if root.key == None:
        print("Tree is empty!")
    else:
        G = Digraph(format='png')
        G = buildGraph(G, root)
        display(Image(G.render()))