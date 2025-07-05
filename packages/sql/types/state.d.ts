export type ColumnDetails = {
    type: string;
    isNullable: boolean;
};
export type ColumnRecord = Record<string, ColumnDetails>;
export type TableRecord = Record<string, ColumnRecord>;
export type GeneratedSchema = {
    tables: TableRecord;
};
export type InitialGeneratedSchema = {
    tables: {};
};
export type CurrentColumn = {
    name: string;
    type: string;
    isNullable: boolean;
};
export type CreateTableState = {
    currentTableName: string;
    currentColumns: CurrentColumn[];
};
export type InitialCreateTableState = {
    currentTableName: never;
    currentColumns: CurrentColumn[];
};
export type QueryState = {
    currentTableName: string;
    allColumns: boolean;
    specificColumns: string[];
};
export type InitialQueryState = {
    currentTableName: never;
    allColumns: false;
    specificColumns: [];
};
export type AlterTableState = {
    currentTableName: string;
};
export type InitialAlterTableState = {
    currentTableName: never;
};
export type DropTableState = {
    currentTableName: string;
};
export type InitialDropTableState = {
    currentTableName: never;
};
