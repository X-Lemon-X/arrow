// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// #include <arrow/csv/api.h>
// #include <arrow/io/api.h>
// #include <arrow/ipc/api.h>
// #include <arrow/pretty_print.h>
// #include <arrow/result.h>
// #include <arrow/status.h>
// #include <arrow/table.h>
// #include <iostream>

// using arrow::Status;

// namespace {

// Status RunMain(int argc, char** argv) {
//   const char* csv_filename = "test.csv";
//   const char* arrow_filename = "test.arrow";

//   std::cerr << "* Reading CSV file '" << csv_filename << "' into table" << std::endl;
//   ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(csv_filename));
//   ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
//                                              arrow::io::default_io_context(), input_file,
//                                              arrow::csv::ReadOptions::Defaults(),
//                                              arrow::csv::ParseOptions::Defaults(),
//                                              arrow::csv::ConvertOptions::Defaults()));
//   ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

//   std::cerr << "* Read table:" << std::endl;
//   ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, {}, &std::cerr));

//   std::cerr << "* Writing table into Arrow IPC file '" << arrow_filename << "'"
//             << std::endl;
//   ARROW_ASSIGN_OR_RAISE(auto output_file,
//                         arrow::io::FileOutputStream::Open(arrow_filename));
//   ARROW_ASSIGN_OR_RAISE(auto batch_writer,
//                         arrow::ipc::MakeFileWriter(output_file, table->schema()));
//   ARROW_RETURN_NOT_OK(batch_writer->WriteTable(*table));
//   ARROW_RETURN_NOT_OK(batch_writer->Close());

//   return Status::OK();
// }

// }  // namespace

// int main(int argc, char** argv) {
//   Status st = RunMain(argc, argv);
//   if (!st.ok()) {
//     std::cerr << st << std::endl;
//     return 1;
//   }
//   return 0;
// }


//-------------------------------------------------------------------------------------------------------------------------------------

#include <arrow/api.h>


#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

#include <arrow/filesystem/filesystem.h>

#include <arrow/ipc/writer.h>

#include <arrow/util/iterator.h>

#include <memory>
#include <parquet/arrow/writer.h>

#include "arrow/compute/expression.h"
#include <arrow/compute/cast.h>

#include <arrow/acero/options.h>
#include <arrow/acero/exec_plan.h>


#include <iostream>
#include <vector>
#include <tuple>
#include <type_traits>
#include <random>

namespace ds = arrow::dataset;
namespace fs = arrow::fs;
namespace cp = arrow::compute;
namespace ac = arrow::acero;
  

template <typename VariableType>
struct TypeTraits
{
  using Type             = std::remove_cv_t<VariableType>;    
  using ArrowValueType   = typename arrow::CTypeTraits<Type>::ArrowType;
  using ArrowBuilderType = typename arrow::CTypeTraits<Type>::BuilderType;
  using ArrowArrayType   = typename arrow::CTypeTraits<Type>::ArrayType;
};

template <typename VariableType>
struct VectorToArray{
  using Type             = std::remove_cv_t<VariableType>;    
  using ArrowValueType   = typename TypeTraits<VariableType>::ArrowValueType;
  using ArrowBuilderType = typename TypeTraits<VariableType>::ArrowBuilderType;
  using ArrowArrayType   = typename TypeTraits<VariableType>::ArrowArrayType;
 
  static std::shared_ptr<ArrowArrayType> vector_to_array(std::vector<VariableType> &vector){
    ArrowBuilderType builder;
    for (auto &&i : vector){
      (void)builder.Append(i);
    }
    std::shared_ptr<ArrowArrayType> a;
    (void)builder.Finish(&a);
    return a;
  };
};

arrow::Status tables(){
    // tabela 
  ///    nazwa EID id jakes
  ///  nazwa GID kolumna jako skalar lista id
  // EID  GID
  // 1    1,2,3
  // 2    4,5
  // 3    6

  // tabela
  // GID id
  // jakeis pole

  // GID  TOA
  // 1    10
  // 2    20
  // 3    30
  // 4    40
  // 5    50
  // 6    60

  // GID  TOA  EID
  // 1    10    1
  // 2    20    1
  // 3    30    1
  // 4    40    3
  // 5    50    3
  // 6    60    3
  std::vector<int> gid_vector = {1,2,3,4,5,6};
  std::vector<int> toa_vector = {10,20,30,40,50,60};

  std::vector<int> eid_vector = {1,2,3};
  std::vector<int> gid_1_vector = {1,2,3};
  std::vector<int> gid_2_vector = {4,5};
  std::vector<int> gid_3_vector = {6};

  std::vector<int> eid_test = {1,2,3};

  auto gid_array = VectorToArray<decltype(gid_vector)::value_type>::vector_to_array(gid_vector);
  auto toa_array = VectorToArray<decltype(toa_vector)::value_type>::vector_to_array(toa_vector);

  auto eid_array =  VectorToArray<decltype(eid_vector)::value_type>::vector_to_array(eid_vector);
  // dla tetsu eid gid
  // auto etets_array =  VectorToArray<decltype(eid_test)::value_type>::vector_to_array(eid_test);
  // auto list_builder_array = std::static_pointer_cast<arrow::Array>(etets_array);

  auto list_gid_1_array = VectorToArray<decltype(gid_1_vector)::value_type>::vector_to_array(gid_1_vector);
  auto list_gid_2_array = VectorToArray<decltype(gid_1_vector)::value_type>::vector_to_array(gid_2_vector);
  auto list_gid_3_array = VectorToArray<decltype(gid_1_vector)::value_type>::vector_to_array(gid_3_vector);
  
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::ListBuilder lb (pool,std::make_shared<arrow::Int32Builder>(pool));
  arrow::Int32Builder *list_builder  = (static_cast<arrow::Int32Builder*>(lb.value_builder()));
  lb.Append();
  list_builder->AppendValues(gid_1_vector.data(),gid_1_vector.size());
  lb.Append();
  list_builder->AppendValues(gid_2_vector.data(),gid_2_vector.size());
  lb.Append();
  list_builder->AppendValues(gid_3_vector.data(),gid_3_vector.size());
  std::shared_ptr<arrow::Array> list_builder_array;
  lb.Finish(&list_builder_array);

  auto gid_a = std::static_pointer_cast<arrow::Array>(gid_array);
  auto toa_a = std::static_pointer_cast<arrow::Array>(toa_array);
  auto eid_a = std::static_pointer_cast<arrow::Array>(eid_array);

  std::vector<std::shared_ptr<arrow::Field>> gid_table_s = {
    arrow::field("GID",arrow::int32()),
    arrow::field("TOA",arrow::int32())
  };
  auto git_table_schema = std::make_shared<arrow::Schema>(gid_table_s);
  
  std::vector<std::shared_ptr<arrow::Field>> eid_table_s = {
    arrow::field("EID",arrow::int32()),
    arrow::field("GID",arrow::list(arrow::int32()))
    // arrow::field("GID",arrow::int32()),
  };
    // arrow::field("EID",arrow::int64()),
  auto eid_table_schema = std::make_shared<arrow::Schema>(eid_table_s);

  auto gid_batch = arrow::Table::Make(git_table_schema,{gid_a,toa_a},gid_a->length());
  auto eid_batch = arrow::Table::Make(eid_table_schema,{eid_a,list_builder_array},eid_a->length());



  std::cout << gid_batch->ToString() << std::endl;
  std::cout << "*********************************************************"<< std::endl;
  std::cout << eid_batch->ToString() << std::endl;

  // Comging tables
  std::cout << "[JOIN TABLES] *********************************************************"<< std::endl;
  arrow::dataset::internal::Initialize();
  
  // cp::Expression a_times_b = cp::call("multiply", {cp::field_ref("GID"), cp::field_ref("TOA") });


  auto options1  = std::make_shared<arrow::dataset::ScanOptions>();
  options1->projection = cp::project({},{});
  auto options2  = std::make_shared<arrow::dataset::ScanOptions>();
  options2->projection = cp::project({},{});

  auto dataset_gid  = std::make_shared<arrow::dataset::InMemoryDataset>(gid_batch);
  auto dataset_eid  = std::make_shared<arrow::dataset::InMemoryDataset>(eid_batch);
  auto scan_node_options_gid = arrow::dataset::ScanNodeOptions{dataset_gid, options1};
  auto scan_node_options_eid = arrow::dataset::ScanNodeOptions{dataset_eid, options2};
  auto akgr = arrow::compute::ScalarAggregateOptions();

  arrow::acero::HashJoinNodeOptions join_opts{arrow::acero::JoinType::INNER,
                                              /*in_left_keys=*/{"GID"},
                                              /*in_right_keys=*/{"GID"},
                                              /*filter*/ arrow::compute::literal(true),
                                              /*output_suffix_for_left*/ "_l",
                                              /*output_suffix_for_right*/ "_r"};

  cp::Expression is_in = cp::call("is_in", {cp::field_ref("GID"), cp::field_ref("GIDL") });

  ac::Declaration scanG{"scan", std::move(scan_node_options_gid),"scan"};
  ac::Declaration scanE{"scan", std::move(scan_node_options_eid),"scan"};
  // ac::Declaration hash{"hashjoin", {std::move(scanG),std::move(scanE)}, join_opts,"project-sth-1"};
  ac::Declaration hash{"project", {std::move(scanG),std::move(scanE)},,"project-sth-1"};


  std::cout << hash.IsValid() << std::endl;

  auto maybe_resp_table = ac::DeclarationToTable(std::move(hash));

  if(!maybe_resp_table.ok()){
    std::cout << "Error Declaring table:  " << maybe_resp_table.status().ToString() << std::endl;
    return arrow::Status::OK();
  }
  auto response_table = maybe_resp_table.ValueOrDie();
  std::cout << "Results : " << response_table->ToString() << std::endl;

  return arrow::Status::OK();
}


int main(int argc, char** argv) {
  tables();
  return 0;
  // // auto status = fun();

  // // make two random genrators for two different distributions
  // std::random_device rd;
  // std::mt19937 gen(rd());
  // std::uniform_real_distribution<float> dis(0,1);

  // // create a vector of size 1000 with random numbers
  // std::vector<int> ids;
  // std::vector<float> rows1;
  // std::vector<float> rows2;
  // // std::vector<float> base_signal;
  // // std::vector<float> base_signal = {
  // //   0.3, 0.5, 0.3, 0.7, 0.2, 0.9, 0.4, 0.8, 0.6, 0.0, 0.1, 0.5
  // //   };
  // // size_t length = 1000;
  // // size_t delay = 2;
  // // for (size_t i = 0; i < length; i++){
  // //   base_signal.push_back(dis(gen));
  // // }

  // // write the signal to rows1 but cut the last dealy elements
  // // for (size_t i = 0; i < base_signal.size()-delay; i++){
  // //   rows1.push_back(base_signal[i]);
  // //   rows2.push_back(base_signal[i+delay]);
  // // }
  // // now we will get koeracje between the two signals of T=delay
  
  // // convert the vectors to arrow arrays
  // auto rows1_array = VectorToArray<decltype(rows1)::value_type>::vector_to_array(rows1);
  // auto rows2_array = VectorToArray<decltype(rows2)::value_type>::vector_to_array(rows2);
  // auto r1 = std::static_pointer_cast<arrow::Array>(rows1_array);
  // auto r2 = std::static_pointer_cast<arrow::Array>(rows2_array);

  // std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
  //   arrow::field("value",arrow::float64()),
  //   arrow::field("value2",arrow::float64())
  // };
  // auto schema = std::make_shared<arrow::Schema>(schema_vector);
  
  // // std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema,{r1, r2});

  // arrow::dataset::internal::Initialize(); // this hase to be added or  ac::Declaration scan{"scan", std::move(scan_node_options)}; won't work

  // auto record_batch = arrow::RecordBatch::Make(schema,rows1_array->length(),{r1,r2});
  // auto maybe_table = arrow::Table::FromRecordBatches({record_batch});
  // if(!maybe_table.ok()){
  //   std::cout << maybe_table.status().ToString() << std::endl;
  //   return 1;
  // }
  // auto table = maybe_table.ValueOrDie();

  // auto record_batch2 = arrow::RecordBatch::Make(schema,rows1_array->length(),{r1,r2});
  // auto maybe_table2 = arrow::Table::FromRecordBatches({record_batch});
  // if(!maybe_table2.ok()){
  //   std::cout << maybe_table2.status().ToString() << std::endl;
  //   return 1;
  // }
  // auto table2 = maybe_table2.ValueOrDie();

  // // auto dataset  = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  // // auto options  = std::make_shared<arrow::dataset::ScanOptions>();

  // auto dataset2 = std::make_shared<arrow::dataset::InMemoryDataset>(table2);
  // // auto options2 = std::make_shared<arrow::dataset::ScanOptions>();

  // std::cout << "Table:" << table->ToString() << std::endl;
  
  // // projection
  // cp::Expression a_times_b = cp::call("multiply", {cp::field_ref("value"), cp::field_ref("value2") });
  // // cp::Expression greater = cp::greater(cp::field_ref("value"),cp::field_ref("out"));
  // // cp::Expression filte_out_greter = cp::greater(cp::field_ref("multiply(value, value2)"),cp::literal(0.5));
  // cp::Expression filte_out_greter = cp::greater(cp::field_ref("out"),cp::literal(0.5));
  // cp::Expression ref1 = cp::field_ref("value2");
  


  // auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
  // // auto scan_node_options2 = arrow::dataset::ScanNodeOptions{dataset2, options2};
  // // ac::Declaration scan2{"scan", std::move(scan_node_options2),"scan2"};

  // ac::Declaration scan{"scan", std::move(scan_node_options),"scan"};
  // ac::Declaration project{"project", {std::move(scan)}, ac::ProjectNodeOptions( { a_times_b,cp::field_ref("value"),cp::field_ref("value2")}, {"out","value","value2"}  ),"project-sth-1"};
  // ac::Declaration project2{ "filter", {std::move(project)}, ac::FilterNodeOptions( {filte_out_greter} ), "project-sth-2"};

  // std::cout << scan.IsValid() << std::endl;
  // std::cout << project.IsValid() << std::endl;
  // // std::cout << project2.IsValid() << std::endl;

  // // ac::Declaration plan =
  // //     ac::Declaration::Sequence({
  // //       {"scan", std::move(scan_node_options)},
  // //       {"project", ac::ProjectNodeOptions({a_times_b}, {"out","value","value2"})},
  // //       {"filter", ac::FilterNodeOptions({greater})}
  // //     });

  // auto maybe_resp_table = ac::DeclarationToTable(std::move(project2));
  // if(!maybe_resp_table.ok()){
  //   std::cout << "Error Declaring table:  " << maybe_resp_table.status().ToString() << std::endl;
  //   return 1;
  // }

  // auto response_table = maybe_resp_table.ValueOrDie();
  // std::cout << "Results : " << response_table->ToString() << std::endl;
  // auto maybe_file_writer = arrow::ipc::MakeFileWriter(arrow::io::FileOutputStream::Open("test.arrow").ValueOrDie(),response_table->schema());
  // if(!maybe_file_writer.ok()){
  //   std::cout << "Error creating file writer:  " << maybe_file_writer.status().ToString() << std::endl;
  //   return 1;
  // }

  // auto file_writer = maybe_file_writer.ValueOrDie();
  // auto status = file_writer->WriteTable(*response_table);
  // if(!status.ok()){
  //   std::cout << "Error writing table:  " << status.ToString() << std::endl;
  //   return 1;
  // }
  

  //**************************************************************************************** */

  // // using the arrays to create two tables
  // std::vector<std::shared_ptr<arrow::Array>> arrays1 = {rows1_array};
  // std::vector<std::shared_ptr<arrow::Array>> arrays2 = {rows2_array};
  // auto schema = arrow::schema({arrow::field("value",arrow::float64())});
  // auto table1 = arrow::Table::Make(schema,arrays1);
  // auto table2 = arrow::Table::Make(schema,arrays2);
  // std::cout << "Table1:" << table1->ToString() << std::endl;
  // std::cout << "Table2:" << table2->ToString() << std::endl;

  // // Using arrow accero to add the two tables
  // auto tb1= table1->GetColumnByName(std::string("value"))->chunk(0);
  // auto tb2 = table2->GetColumnByName(std::string("value"))->chunk(0);

  // 1. Create a new ExecPlan object.



  // 2. Add sink nodes to your graph of Declaration objects (this is the only type you will need to create declarations for sink nodes)
  // 3. Use Declaration::AddToPlan() to add your declaration to your plan (if you have more than one output then you will not be able to use this method and will need to add your nodes one at a time)
  // 4. Validate the plan with ExecPlan::Validate()
  // 5. Start the plan with ExecPlan::StartProducing()
  // 6. Wait for the future returned by ExecPlan::finished() to complete.

  // auto added_table = arrow::compute::CallFunction("add",{rows1_array,rows2_array});
  // auto array = added_table.ValueOrDie();
  // auto dat = std::move(array).make_array();
  // std::cout << dat->ToString() << std::endl;
  // // find koleration of the two signals
  // auto koleration = arrow::compute::CallFunction("correlation",{rows1_array,rows2_array});
  // auto koleration_array = koleration.ValueOrDie();
  // auto koleration_dat = std::move(koleration_array).make_array();
  // std::cout << koleration_dat->ToString() << std::endl;




  // std::vector<int> v = {1,2,3,4,5,6,7,8,9,10};
  // auto array = VectorToArray<decltype(v)::value_type>::vector_to_array(v);
  // std::cout << array->ToString() << std::endl;

  // std::vector<float> vf = {1.1, 2.2, 3.3, 4.4, 5.5};
  // auto float_array = VectorToArray<decltype(vf)::value_type>::vector_to_array(vf);
  // std::cout << float_array->ToString() << std::endl;

  // std::vector<std::string> vs = {"one", "two", "three", "four", "five"};
  // auto string_array = VectorToArray<decltype(vs)::value_type>::vector_to_array(vs);
  // std::cout << string_array->ToString() << std::endl;
  


  // auto result = arrow_rows_with_data_struct();
  // if(!result.ok()){
  //   return 1;
  // }
  // auto table = result.ValueOrDie(); 
  
  // // auto ids = std::static_pointer_cast<arrow::UInt64Array>(table->column(0)->chunk(0));
  // // auto components = std::static_pointer_cast<arrow::FloatArray>(table->column(1)->chunk(0));
  // print_table(table);
  // auto column_id = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName(std::string("id"))->chunk(0));
  // auto column_val=std::static_pointer_cast<arrow::FloatArray>(table->GetColumnByName(std::string("value"))->chunk(0));
  // auto column_val2=std::static_pointer_cast<arrow::FloatArray>(table->GetColumnByName(std::string("value2"))->chunk(0));
  
  

  // std::cout << "Adding" << std::endl;
  // auto incremented_datum = arrow::compute::CallFunction("subtract",{column_val,column_val2});
  // std::cout << "Added" << std::endl;
  // auto array = incremented_datum.ValueOrDie();
  // auto dat = std::move(array).make_array();
  // std::cout << dat->ToString() << std::endl;






  // // std::vector<data_row> rows;
  // for (size_t i = 0; i < table->num_rows(); i++){
  //   data_row dr = {column_id->Value(i), column_val->Value(i)};

  //   std::cout << "ID:" << dr.id << "  VALUE:" << dr.value << std::endl;
  //   // rows.push_back(dr);
  // }
  


  // if (!status.ok()) {
  //   std::cerr << status.ToString() << std::endl;
  //   return EXIT_FAILURE;
  // }
  return EXIT_SUCCESS;

  // std::string uri = argv[1];
  // std::string format_name = argv[2];
  // std::string mode = argc > 3 ? argv[3] : "no_filter";

  // auto status = RunDatasetDocumentation(format_name, uri, mode);
  // if (!status.ok()) {
  //   std::cerr << status.ToString() << std::endl;
  //   return EXIT_FAILURE;
  // }
  // return EXIT_SUCCESS;
}
