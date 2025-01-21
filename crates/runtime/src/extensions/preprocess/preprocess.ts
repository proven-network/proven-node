import ts from "typescript";

export default function preprocess(source: string): string {
  const sourceFile = ts.createSourceFile(
    "module.ts",
    source,
    ts.ScriptTarget.Latest,
    true
  );

  const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
  const result = ts.transform(sourceFile, [transformer]);

  const transformedSourceFile = result.transformed[0];
  return printer.printFile(transformedSourceFile);
}

function transformer(
  context: ts.TransformationContext
): ts.Transformer<ts.SourceFile> {
  return (rootNode: ts.SourceFile) => {
    let hasRunWithOptionsImport = false;

    // Check for existing import of runWithOptions
    for (const node of rootNode.statements) {
      if (ts.isImportDeclaration(node)) {
        const importPath = (node.moduleSpecifier as ts.StringLiteral).text;
        if (importPath === "proven:handler") {
          const clause = node.importClause;
          if (
            clause &&
            clause.namedBindings &&
            ts.isNamedImports(clause.namedBindings)
          ) {
            for (const element of clause.namedBindings.elements) {
              if (element.name.text === "runWithOptions") {
                hasRunWithOptionsImport = true;
                break;
              }
            }
          }
        }
      }
    }

    const importStatement = ts.factory.createImportDeclaration(
      undefined,
      ts.factory.createImportClause(
        false,
        undefined,
        ts.factory.createNamedImports([
          ts.factory.createImportSpecifier(
            false,
            undefined,
            ts.factory.createIdentifier("runWithOptions")
          ),
        ])
      ),
      ts.factory.createStringLiteral("proven:handler")
    );

    function visitor(node: ts.Node): ts.Node {
      // Make function parameters async in run* calls
      if (
        ts.isCallExpression(node) &&
        ts.isIdentifier(node.expression) &&
        /^run(WithOptions|OnHttp|OnRadixEvent|OnSchedule|OnProvenEvent)$/.test(
          node.expression.text
        )
      ) {
        const args = [...node.arguments];
        if (
          args[0] &&
          (ts.isArrowFunction(args[0]) || ts.isFunctionExpression(args[0]))
        ) {
          args[0] = createAsyncArrowFunction(args[0]);
        }
        return ts.factory.updateCallExpression(
          node,
          node.expression,
          node.typeArguments,
          args
        );
      }

      // Handle exported declarations
      if (
        ts.isVariableStatement(node) &&
        node.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)
      ) {
        const declarations = node.declarationList.declarations.map((decl) => {
          if (ts.isVariableDeclaration(decl)) {
            // Handle direct function assignments
            if (
              decl.initializer &&
              (ts.isArrowFunction(decl.initializer) ||
                ts.isFunctionExpression(decl.initializer))
            ) {
              return wrapInRunWithOptions(decl);
            }
            // Handle function calls that need async parameters
            if (decl.initializer && ts.isCallExpression(decl.initializer)) {
              return ts.factory.updateVariableDeclaration(
                decl,
                decl.name,
                decl.exclamationToken,
                decl.type,
                visitor(decl.initializer) as ts.Expression
              );
            }
          }
          return decl;
        });

        return ts.factory.updateVariableStatement(
          node,
          node.modifiers,
          ts.factory.updateVariableDeclarationList(
            node.declarationList,
            declarations
          )
        );
      }

      if (ts.isExportDeclaration(node)) {
        return node;
      }

      if (ts.isExportAssignment(node)) {
        // Handle default exports
        const expression = node.expression;
        if (
          ts.isArrowFunction(expression) ||
          ts.isFunctionExpression(expression)
        ) {
          return ts.factory.createExportAssignment(
            undefined,
            undefined,
            ts.factory.createCallExpression(
              ts.factory.createIdentifier("runWithOptions"),
              [],
              [
                createAsyncArrowFunction(expression),
                ts.factory.createObjectLiteralExpression([]),
              ]
            )
          );
        }
        return node;
      }

      if (
        ts.isFunctionDeclaration(node) &&
        node.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)
      ) {
        return createExportedVariableFromFunction(node);
      }

      return ts.visitEachChild(node, visitor, context);
    }

    function wrapInRunWithOptions(
      decl: ts.VariableDeclaration
    ): ts.VariableDeclaration {
      const func = decl.initializer as ts.FunctionExpression | ts.ArrowFunction;
      const asyncArrowFunc = createAsyncArrowFunction(func);

      return ts.factory.updateVariableDeclaration(
        decl,
        decl.name,
        decl.exclamationToken,
        decl.type,
        ts.factory.createCallExpression(
          ts.factory.createIdentifier("runWithOptions"),
          [],
          [asyncArrowFunc, ts.factory.createObjectLiteralExpression([])]
        )
      );
    }

    function createExportedVariableFromFunction(
      func: ts.FunctionDeclaration
    ): ts.VariableStatement {
      const asyncArrowFunc = createAsyncArrowFunction(func);

      return ts.factory.createVariableStatement(
        [ts.factory.createModifier(ts.SyntaxKind.ExportKeyword)],
        ts.factory.createVariableDeclarationList(
          [
            ts.factory.createVariableDeclaration(
              func.name || ts.factory.createIdentifier("defaultName"),
              undefined,
              undefined,
              ts.factory.createCallExpression(
                ts.factory.createIdentifier("runWithOptions"),
                [],
                [asyncArrowFunc, ts.factory.createObjectLiteralExpression([])]
              )
            ),
          ],
          ts.NodeFlags.Const
        )
      );
    }

    function createAsyncArrowFunction(
      func: ts.FunctionDeclaration | ts.FunctionExpression | ts.ArrowFunction
    ): ts.ArrowFunction {
      // Don't wrap return type in Promise if it's already a Promise
      const returnType =
        func.type &&
        ts.isTypeReferenceNode(func.type) &&
        func.type.typeName.getText() === "Promise"
          ? func.type
          : ts.factory.createTypeReferenceNode("Promise", [
              func.type ||
                ts.factory.createKeywordTypeNode(ts.SyntaxKind.AnyKeyword),
            ]);

      return ts.factory.createArrowFunction(
        [ts.factory.createModifier(ts.SyntaxKind.AsyncKeyword)],
        func.typeParameters,
        func.parameters,
        returnType,
        ts.factory.createToken(ts.SyntaxKind.EqualsGreaterThanToken),
        func.body && ts.isBlock(func.body)
          ? func.body
          : ts.factory.createBlock([
              ts.factory.createReturnStatement(
                func.body || ts.factory.createIdentifier("undefined")
              ),
            ])
      );
    }

    const visited = ts.visitEachChild(rootNode, visitor, context);
    return ts.factory.updateSourceFile(
      rootNode,
      hasRunWithOptionsImport
        ? visited.statements
        : [importStatement, ...visited.statements]
    );
  };
}
