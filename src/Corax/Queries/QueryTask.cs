using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public delegate AbstractQueryTask QueryInstanceGenerator(QueryDefinition definition, IndexSearcher searcher, 
        //TODO: Not it
        Dictionary<string, string> parameters);
    
    public abstract class AbstractQueryTask
    {
        public QueryDefinition Definition { get; }
        
        protected AbstractQueryTask(QueryDefinition definition)
        {
            Definition = definition;
        }

        public static AbstractQueryTask Create<T>(QueryDefinition definition, T query)
            where T : struct, IIndexMatch
        {
            return new QueryTask<T>(definition, query);
        }

        public abstract int Execute(Span<long> results);
    }
    
    public class QueryTask<T> : AbstractQueryTask
        where T : struct, IIndexMatch
    {
        private T _queryInstance;

        internal QueryTask(QueryDefinition definition, T queryInstance) : base(definition)
        {
            _queryInstance = queryInstance;
        }
        
        public override int Execute(Span<long> results)
        {
            int i = 0;
            ref var instance = ref _queryInstance;

            while (instance.MoveNext(out var v) && i < results.Length)
            {
                results[i++] = v;
            }

            return i;
        }
    }
}
