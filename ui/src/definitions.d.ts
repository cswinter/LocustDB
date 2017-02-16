/** GraphMVC model definitions **/
declare interface GraphItemData {
  id?: GraphItemId;
  graph_type?: number;
}

declare type GraphItemId = number;

declare type GraphStoreState = GraphItemData[];
