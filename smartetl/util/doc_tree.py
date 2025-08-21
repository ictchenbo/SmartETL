

class Node:
    def __init__(self, data: dict, parent=None):
        self.payload = data
        self.number = data.get("number")
        self.parent = parent
        self.brother = None
        self.children = []
        if parent:
            if parent.children:
                self.brother = parent.children[-1]
            parent.children.append(self)

    def display(self, level=0):
        for i in range(level):
            print('  ', end='')
        if self.number:
            print(self.number, end=' ')
        print(self.payload.get("title") or self.payload.get("content") or "<ROOT>")
        for child in self.children:
            child.display(level+1)

    @staticmethod
    def from_json(doc_root: dict, parent=None):
        node = Node(doc_root, parent)
        for child in doc_root.get("children", []):
            Node.from_json(child, node)

        return node


def find_parent(node: Node, old_parent: Node):
    if node.brother and node.number.startswith(node.brother.number):
        node.parent.remove(node)
        brother = node.brother
        node.brother = brother.children[-1] if brother.children else None
        brother.children.append(node)
        node.parent = brother
    else:
        pass


def adjust_tree(node: Node):
    for child in node.children:
        adjust_tree(child)

    number = node.number
    if number and node.parent:
        parent = node.parent
        parent_should_move = False
        if not parent.number or not number.startswith(parent.number):
            parent = node.parent
            grandparent = parent.parent
            if grandparent:
                # 确定位置
                index = grandparent.children.index(parent)
                # 解除关系
                parent.children.remove(node)
                if index > 0:
                    uncle = grandparent.children[index-1]
                    if uncle.number and number.startswith(uncle.number):
                        node.parent = uncle
                        uncle.children.append(node)
                else:
                    node.parent = grandparent
                    grandparent.children.insert(index, node)
            parent_should_move = True

        if not parent.number and parent_should_move:
            grandparent = parent.parent
            if grandparent:
                # 确定位置
                index = grandparent.children.index(parent)
                if index > 0:
                    grandparent.children.remove(parent)
                    grandparent.children[index-1].payload["content"] += parent.payload["title"] + '\n' + parent.payload.get("content")
                    del parent


if __name__ == '__main__':
    import json
    doc = json.load(open('../../data/arxiv/mineru.resultv3.json', encoding='utf8'))
    root = doc['paper']
    tree = Node.from_json(root)
    tree.display()
    print('------------------adjust-------------')
    adjust_tree(tree)
    tree.display()
