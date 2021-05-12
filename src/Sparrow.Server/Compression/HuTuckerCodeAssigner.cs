using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Sparrow.Server.Compression
{
    internal sealed class HuTuckerCodeAssigner
    {
        internal class Node
        {
            public int LeftIdx;
            public int RightIdx;

            public Node LeftChild;
            public Node RightChild;

            public Node(int idx)
            {
                LeftIdx = idx;
                RightIdx = idx;
            }

            public Node (int leftIdx, int rightIdx, Node leftChild, Node rightChild)
            {
                LeftIdx = leftIdx;
                RightIdx = rightIdx;
                LeftChild = leftChild;
                RightChild = rightChild;
            }

            public bool IsLeaf => LeftChild == null;
        }

        private readonly FastList<int> _cached_node_idx_list = new();

        // For now we allocate, we don't need to do so. 
        private FastList<SymbolFrequency> _symbolsList = new();
        private FastList<int> _codeLenList = new();
        private FastList<Node> node_list_ = new();
        private Node root_;

        public FastList<SymbolCode> AssignCodes(in FastList<SymbolFrequency> frequency, FastList<SymbolCode> symbol_code_list = null)
        {
            Clear();

            if (symbol_code_list == null)
                symbol_code_list = new FastList<SymbolCode>();
            else
                symbol_code_list.Clear();

            // Initialize the table of symbols.
            for (int i = 0; i < frequency.Count; i++)
            {
                _symbolsList.Add(new SymbolFrequency(frequency[i].StartKey, frequency[i].Frequency));
            }

            GenerateOptimalCode();
            BuildBinaryTree();

            for (int i = 0; i < _symbolsList.Count; i++)
            {
                Code code = Lookup(i);
                symbol_code_list.Add(new SymbolCode(_symbolsList[i].StartKey, code));
            }

            return symbol_code_list;
        }

        private Code Lookup(int idx)
        {
            Code code = default;

            Node n = root_;
            while (!n.IsLeaf)
            {
                code.Value <<= 1;
                if (idx > n.LeftChild.RightIdx)
                {
                    code.Value += 1;
                    n = n.RightChild;
                }
                else
                {
                    n = n.LeftChild;
                }
                code.Length++;
            }
            return code;
        }

        private void BuildBinaryTree()
        {
            // Initialize all leaf nodes.
            for (int i = 0; i < _codeLenList.Count; i++)
                node_list_.Add(new Node(i));

            int[] tmpCodeLens = ArrayPool<int>.Shared.Rent(_codeLenList.Count);
            for (int i = 0; i < _codeLenList.Count; i++)
            {
                tmpCodeLens[i] = _codeLenList[i];
            }

            int maxCodeLen = GetMaxCodeLen();

            FastList<int> nodeIdxList = _cached_node_idx_list;
            for (int len = maxCodeLen; len > 0; len--)
            {
                nodeIdxList.Clear();
                for (int i = 0; i < tmpCodeLens.Length; i++)
                {
                    if (tmpCodeLens[i] == len)
                        nodeIdxList.Add(i);
                }
                for (int i = 0; i < nodeIdxList.Count; i += 2)
                {
                    int idx1 = nodeIdxList[i];
                    int idx2 = nodeIdxList[i + 1];

                    // Merge Nodes.
                    Node leftNode = node_list_[idx1];
                    Node rightNode = node_list_[idx2];
                    Node newNode = new Node(leftNode.LeftIdx, rightNode.RightIdx, leftNode, rightNode);
                    node_list_[idx1] = newNode;
                    node_list_[idx2] = null;

                    tmpCodeLens[idx1] = len - 1;
                    tmpCodeLens[idx2] = 0;
                }
            }
            root_ = node_list_[0];
        }

        private int GetMaxCodeLen()
        {
            int maxLen = 0;
            for (int i = 0; i < _codeLenList.Count; i++)
            {
                if (_codeLenList[i] > maxLen)
                    maxLen = _codeLenList[i];
            }
            return maxLen;
        }

        private void GenerateOptimalCode()
        {
            int n = _symbolsList.Count;

            int[] L = ArrayPool<int>.Shared.Rent(n);
            long[] P = ArrayPool<long>.Shared.Rent(n);
            int[] s = ArrayPool<int>.Shared.Rent(n);
            int[] d = ArrayPool<int>.Shared.Rent(n);

            long maxp = 1;
            for (int k = 0; k < n; k++)
            {
                L[k] = 0;
                P[k] = _symbolsList[k].Frequency;
                maxp += _symbolsList[k].Frequency;
            }

            for (int m = 0; m < n - 1; m++)
            {
                int i = 0, i1 = 0, i2 = 0;
                long pmin = maxp;
                int sumL = -1;
                while (i < n - 1)
                {
                    if (P[i] == 0)
                    {
                        i++;
                        continue;
                    }

                    int j1 = i, j2 = -1;
                    long min1 = P[i], min2 = maxp;
                    int minL1 = L[i], minL2 = -1;

                    int j = 0;
                    for (j = i + 1; j < n; j++)
                    {
                        if (P[j] == 0)
                            continue;
                        if (P[j] < min1 || (P[j] == min1 && L[j] < minL1))
                        {
                            min2 = min1;
                            j2 = j1;
                            minL2 = minL1;
                            min1 = P[j];
                            j1 = j;
                            minL1 = L[j];
                        }
                        else if (P[j] < min2 || (P[j] == min2 && L[j] < minL2))
                        {
                            min2 = P[j];
                            j2 = j;
                            minL2 = L[j];
                        }

                        if (L[j] == 0)
                            break;
                    }

                    long pt = P[j1] + P[j2];
                    int sumLt = L[j1] + L[j2];
                    if (pt < pmin || (pt == pmin && sumLt < sumL))
                    {
                        pmin = pt;
                        sumL = sumLt;
                        i1 = j1;
                        i2 = j2;
                    }

                    i = j;
                }

                if (i1 > i2)
                {
                    int tmp = i1;
                    i1 = i2;
                    i2 = tmp;
                }

                s[m] = i1;
                d[m] = i2;
                P[i1] = pmin;
                P[i2] = 0;
                L[i1] = sumL + 1;
            }

            L[s[n - 2]] = 0;
            for (int m = n - 2; m >= 0; m--)
            {
                L[s[m]] += 1;
                L[d[m]] = L[s[m]];
            }

            for (int k = 0; k < n; k++)
            {
                _codeLenList.Add(L[k]);
            }

            ArrayPool<int>.Shared.Return(L);
            ArrayPool<long>.Shared.Return(P);
            ArrayPool<int>.Shared.Return(s);
            ArrayPool<int>.Shared.Return(d);
        }

        private void Clear()
        {
            _symbolsList.Clear();
            _codeLenList.Clear();
            node_list_.Clear();
            _cached_node_idx_list.Clear();
        }
    }
}
