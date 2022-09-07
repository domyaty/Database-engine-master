package BPTree;

import java.util.Scanner;

public class TestBPTree {

	public static void main(String[] args) {
		BPTree<String> tree = new BPTree<String>(3);
		for (int i = 0; i < 1; i++) {
			Ref r = new Ref(i, i, null);
			tree.insert("a", r);i++;
			Ref r1 = new Ref(i, i, null);
			tree.insert("b", r1);i++;
			Ref r2 = new Ref(i, i, null);
			tree.insert("c", r2);i++;
			Ref r3 = new Ref(i, i, null);
			tree.insert("d", r3);i++;
			Ref r4 = new Ref(i, i, null);
			tree.insert("e", r4);i++;
			Ref r5 = new Ref(i,i, null);
			tree.insert("f", r5);i++;
			Ref r6 = new Ref(i, i, null);
			tree.insert("g", r6);i++;
			Ref r7 = new Ref(i, i, null);
			tree.insert("h", r7);i++;
		}

		System.out.println(tree.toString());

		BPTreeLeafNode l = tree.getLeafNode("a");
		while (l != null) {

			System.out.print(l);
			System.out.print((tree.search((String) l.getFirstKey())).pageNo + "  "
					+ (tree.search((String) l.getFirstKey())).indexInPage);
			l = l.getNext();
			System.out.println();
		}
		
		
//		BPTreeLeafNode b = tree.getLeafNode(5);
//		BPTreeLeafNode c = b.getNext();
//		System.out.println(b.getFirstKey());
//		System.out.println(c.getFirstKey());
//		System.out.println(tree.getFirstLeaf());
//		BPTreeInnerNode<Integer> x = (BPTreeInnerNode)tree.root ;
//		BPTreeInnerNode<Integer> y = (BPTreeInnerNode) x.getChild(0);
//		BPTreeInnerNode<Integer> y1 = (BPTreeInnerNode) x.getChild(1);
//		BPTreeInnerNode<Integer> y2 = (BPTreeInnerNode) x.getChild(2);
//		System.out.println(x.children[2]);
//		System.out.println(y.children[0]);
//		System.out.println(y1.children[1]);
//		System.out.println(y2.children[1]);

//		Scanner sc = new Scanner(System.in);
//		while(true) 
//		{
//			int x = sc.nextInt();
//			if(x == -1)
//				break;
//			tree.insert(x, null);
//			System.out.println(tree.toString());
//		}
//		while(true) 
//		{
//			int x = sc.nextInt();
//			if(x == -1)
//				break;
//			tree.delete(x);
//			System.out.println(tree.toString());
//		}
//		sc.close();
	}
}
