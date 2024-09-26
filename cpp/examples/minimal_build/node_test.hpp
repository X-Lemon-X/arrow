#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
// #include "arrow/util/tracing_internal.h"

// namespace arrow {
// namespace acero {

class PivotListExtendNodeOptions : public arrow::acero::ExecNodeOptions {
public:
  static constexpr std::string_view kName = "pivot_list";
  std::string list_column_field_name;
  PivotListExtendNodeOptions(std::string list_column_field_name){}
};

// } // namespace acero
// } // namespace arrow 

// namespace arrow {

// using internal::checked_cast;

// namespace acero {
// namespace {

class PivotListExtendNode : public arrow::acero::ExecNode, public arrow::acero::TracedNode {
public:
  
  static arrow::Result<std::shared_ptr<arrow::Schema>> MakeOutputSchema(const PivotListExtendNodeOptions& options, const std::shared_ptr<arrow::Schema> &input_schema_) {
    arrow::Result<std::shared_ptr<arrow::Schema>> res = arrow::Result<std::shared_ptr<arrow::Schema>>(input_schema_);
    return input_schema_;
  } 
  //   std::vector<std::shared_ptr<Field>> fields = input_schema->fields();
  //   std::vector<std::shared_ptr<Field>> new_fields;
  //   for (auto field : fields) {
  //     if( field->name() == options_.list_column_field_name) {
  //       if(field->type() != arrow::list()) {
  //         throw std::runtime_error("PivotListExtendNode: list_column_field_name must be a list"); 
  //       }
  //       auto type_of_elements = field->type()->field(0)->type().get();
  //       auto new_field = arrow::field(field->name(), type_of_elements);
  //       new_fields.push_back(new_field);
  //     } else {
  //       new_fields.push_back(std::move(field));
  //     }
  //   }
  //   return std::make_shared<Schema>(new_fields);
  // }

  PivotListExtendNode(
    arrow::acero::ExecPlan *plan, 
    std::vector<arrow::acero::ExecNode*> inputs, 
    std::shared_ptr<arrow::Schema> output_schema, 
    PivotListExtendNodeOptions options
  ):
    arrow::acero::ExecNode(plan, std::move(inputs), {"input"},std::move(output_schema)),
    arrow::acero::TracedNode(this),
    options_(std::move(options))
  {}

  static arrow::Result<ExecNode*> Make(
    arrow::acero::ExecPlan* plan, 
    std::vector<arrow::acero::ExecNode*> inputs, 
    const arrow::acero::ExecNodeOptions& options
    ) 
  {
    RETURN_NOT_OK(arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "PivotListExtendNode"));
    const auto& pivot_options = arrow::internal::checked_cast<const PivotListExtendNodeOptions&>(options);

    // std::vector<BoundRowTemplate> templates;
    // for (const auto& row_template : pivot_options.row_templates) {
    //   ARROW_ASSIGN_OR_RAISE(
    //       BoundRowTemplate bound_template,
    //       BoundRowTemplate::Make(row_template, *inputs[0]->output_schema()));
    //   templates.push_back(std::move(bound_template));
    // }

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> output_schema, PivotListExtendNode::MakeOutputSchema(pivot_options, inputs[0]->output_schema()) );
    return plan->EmplaceNode<PivotListExtendNode>(
      plan, 
      std::move(inputs),
      std::move(output_schema), 
      pivot_options
    );
      // std::move(templates)
  }


  const char* kind_name() const override {
    return "PivotListExtendNode"; 
  }

  arrow::Status StartProducing() override {
    // Start the producer thread
    return arrow::Status::OK();
  }

  arrow::Status StopProducing() override {
    // Stop the producer thread
    return arrow::Status::OK();
  }

  void PauseProducing(arrow::acero::ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  arrow::Status StopProducingImpl() override { 
    return arrow::Status::OK(); 
  }

 

  arrow::Status InputReceived(arrow::acero::ExecNode* input, arrow::ExecBatch batch) override {
    // auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);
    // for (const auto& row_template : templates_) {
    //   ExecBatch template_batch = ApplyTemplate(row_template, batch);
    // }
    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
    return arrow::Status::OK();
  }

  protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "column=[";
    ss << options_.list_column_field_name;
    ss << "]";
    return ss.str();
  }

private:
  PivotListExtendNodeOptions options_;
  std::vector<std::shared_ptr<arrow::DataType>> meas_types_;

};

// }  // namespace

// namespace internal {

  void RegisterPivotLongerNode(arrow::acero::ExecFactoryRegistry* registry) {
    DCHECK_OK(registry->AddFactory("pivot_list",PivotListExtendNode::Make));
  }

// } // namespace internal
// }  // namespace acero
// }  // namespace arrow