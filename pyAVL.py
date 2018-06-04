from graphviz import Digraph
from IPython.display import Image, display


class AVL(object):
    def __init__(self):
        self.root = Node()

    def get(self, dkey):
        return self.__getNode(self.root, dkey)

    def put(self, dkey, dval = None):
        self.__putNode(self.root, dkey, str(dkey) if dval is None else dval)
        unbalanced = self.__balanceCheck(self.root)
        if unbalanced is not None:
            self.root = self.__getRoot(self.__autoRotate(unbalanced))

    def min(self):
        return self.__getMinNode(self.root)

    def max(self):
        return self.__getMaxNode(self.root)

    def remove(self, dkey):
        self.__removeNode(self.root, dkey)
        unbalanced = self.__balanceCheck(self.root)
        if unbalanced is not None:
            self.root = self.__getRoot(self.__autoRotate(unbalanced))

    def print(self):
        self.__prettyPrintTree(self.root)
        
    def __str__(self):
        return self.__strTree(self.root)

    def __strTree(self, droot):
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

    def __getNode(self, droot, dkey):
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

    def __putNode(self, droot, dkey, dval):
        """
        if dkey presents, perform update,
        otherwise perform insertion
        """

        tnode = self.__getNode(droot, dkey)
        if tnode.height == -1:
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
                self.__fixHeight(tnode.left)
            else:
                tnode.right = Node(dkey, dval)
                tnode.right.parent = tnode
                self.__fixHeight(tnode.right)

    def __getMinNode(self, droot):
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

    def __getMaxNode(self, droot):
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

    def __removeNode(self, droot, dkey):
        """
        if dkey exists, remove the corresponding node and return its parent,
        otherwise return None (means failure)
        """
        tnode = self.__getNode(droot, dkey)
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
                        self.__removeNode(temp, temp.key)
                else:
                    # 2 children
                    temp = self.__getMinNode(tnode.right)
                    tnode.copy(temp)
                    self.__removeNode(tnode.right, tnode.key)
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

    def __fixHeight(self, dnode):
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

    def __balanceCheck(self, droot):
        """
        should check after every modification
        return imbalanced node for rotation,
        otherwise return None
        """
        dnode = droot
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
        if root.key == None:
            print("Tree is empty!")
        else:
            G = Digraph(format='png')
            G = self.__buildGraph(G, root)
            display(Image(G.render()))

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