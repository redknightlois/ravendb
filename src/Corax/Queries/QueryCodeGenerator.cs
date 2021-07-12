using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Emit;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Corax.Queries
{
    public struct QueryCodeGenerator
    {

        private int _variableIdx;

        public class Rewriter : CSharpSyntaxRewriter
        {
            private readonly ExpressionSyntax _init;

            public Rewriter(ExpressionSyntax init)
            {
                _init = init;
            }

            public override SyntaxNode? VisitVariableDeclarator(VariableDeclaratorSyntax node)
            {
                return node.WithInitializer(EqualsValueClause(_init));
            }
        }

        public QueryInstanceGenerator Create(QueryDefinition query)
        {
            if (query.Query.Where is null)
            {
                return (definition, searcher, parameters) => searcher.AllDocuments();
            }

            var (resultName, block) = VisitQueryExpression(query.Query.Where, Block());

            var lambda = (ParenthesizedLambdaExpressionSyntax)ParseExpression("(definition, searcher, parameters) => {}");
            ExpressionSyntax final = lambda.WithBody(block
                .AddStatements(ReturnStatement(ParseExpression($"AbstractQueryTask.Create(definition, {resultName})")))
            );

            CompilationUnitSyntax classDecl = ((CompilationUnitSyntax)new Rewriter(final)
                .Visit(ParseCompilationUnit("public class GeneratorImpl { public static QueryInstanceGenerator Generator = null; }")))
                .WithUsings(List(new []{
                    UsingDirective(IdentifierName(typeof(IndexSearcher).Namespace)),
                    UsingDirective(IdentifierName(typeof(object).Namespace)),
                    UsingDirective(IdentifierName(typeof(QueryDefinition).Namespace)),
                    UsingDirective(IdentifierName(typeof(Dictionary<,>).Namespace))
                    }))
                .NormalizeWhitespace();
            
            
            var compilation = CSharpCompilation.Create(
                assemblyName: query.Name,
                syntaxTrees: new[]{ SyntaxFactory.ParseSyntaxTree(classDecl.ToFullString())},
                references: new List<MetadataReference>(IndexCompiler.References)
                {
                    IndexCompiler.CreateMetadataReferenceFromAssembly(typeof(IndexSearcher).Assembly)
                },
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(OptimizationLevel.Debug)
            );

            var asm = new MemoryStream();
            var pdb =  new MemoryStream();

            var result = compilation.Emit(asm, pdb, options: new EmitOptions(debugInformationFormat: DebugInformationFormat.PortablePdb));

            if (result.Success == false)
            {
                ThrowInvalidCompilation(query, result, classDecl);
            }

            asm.Position = 0;
            var assembly = AssemblyLoadContext.Default.LoadFromStream(asm);

            Type type = assembly.GetType("GeneratorImpl");
            return (QueryInstanceGenerator)type!.GetField("Generator")!.GetValue(null);
        }

        private static void ThrowInvalidCompilation(QueryDefinition query, EmitResult result, CompilationUnitSyntax compilationUnit)
        {
            IEnumerable<Diagnostic> failures = result.Diagnostics
                .Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);

            var sb = new StringBuilder();
            sb.AppendLine($"Failed to compile index {query.Name}");
            sb.AppendLine();
            sb.AppendLine(compilationUnit.ToFullString());
            sb.AppendLine();

            foreach (var diagnostic in failures)
                sb.AppendLine(diagnostic.ToString());

            throw new IndexCompilationException(sb.ToString());
        }

        private (string, BlockSyntax) VisitQueryExpression(QueryExpression query, BlockSyntax parent)
        {
            string fieldName;
            string leftName, rightName;
            BlockSyntax leftNode, rightNode;
            StatementSyntax outputNode;

            switch (query)
            {
                case BinaryExpression binary when binary.Operator == OperatorType.And:
                    (leftName, leftNode) = VisitQueryExpression(binary.Left, parent);                    
                    (rightName, rightNode) = VisitQueryExpression(binary.Right, leftNode);
                                        
                    fieldName = $"and_{_variableIdx++}";
                    outputNode = ParseStatement($"var {fieldName} = searcher.And({leftName}, {rightName});");

                    return (fieldName, rightNode.AddStatements(outputNode));

                case BinaryExpression binary when binary.Operator == OperatorType.Or:
                    (leftName, leftNode) = VisitQueryExpression(binary.Left, parent);
                    (rightName, rightNode) = VisitQueryExpression(binary.Right, leftNode);

                    fieldName = $"or_{_variableIdx++}";
                    outputNode = ParseStatement($"var {fieldName} = searcher.Or({leftName}, {rightName});");

                    return (fieldName, rightNode.AddStatements(outputNode));

                case BinaryExpression binary when binary.Operator == OperatorType.Equal && binary.Left.Type == ExpressionType.Field:
                    return VisitFieldExpression(binary, parent);
            }

            throw new NotImplementedException("Not Implemented Yet");
        }

        private (string, BlockSyntax) VisitFieldExpression(BinaryExpression term, BlockSyntax parent)
        {            
            var fieldExpression = (FieldExpression)term.Left;
            switch (term.Right)
            {
                case ValueExpression ve when ve.Value == ValueTokenType.Parameter:
                {
                    // where <FIELD> == $param
                    // ;
                    var fieldName = $"term_{_variableIdx++}";
                    StringSegment stringSegment = ve.Token; // TODO: Need to escape it
                    var syntax = ParseStatement($"var {fieldName} = searcher.TermQuery(\"{fieldExpression.FieldValue}\", parameters[\"{stringSegment}\"]);");
                    return (fieldName, parent.AddStatements(syntax));
                }
                case ValueExpression ve:
                {
                    // where <FIELD> == constant
                    var fieldName = $"term_{_variableIdx++}";
                    // TODO: need to handle non string values
                    StringSegment stringSegment = ve.Token; // TODO: Need to escape it
                    var syntax = ParseStatement($"var {fieldName} = searcher.TermQuery(\"{fieldExpression.FieldValue}\", \"{stringSegment}\");");
                    return (fieldName, parent.AddStatements(syntax));
                }
            }

            throw new NotImplementedException("Not Implemented Yet");
        }

    }
}
