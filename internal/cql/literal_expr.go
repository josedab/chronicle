package cql

func (l *CQLLiteralExpr) nodeType() string { return "LiteralExpr" }

func (l *CQLLiteralExpr) exprType() string { return "literal" }
