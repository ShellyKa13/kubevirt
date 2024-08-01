// Automatically generated by MockGen. DO NOT EDIT!
// Source: strategy.go

package install

import (
	context "context"

	gomock "github.com/golang/mock/gomock"
	v1 "github.com/openshift/api/route/v1"
	v10 "github.com/openshift/api/security/v1"
	v11 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v12 "k8s.io/api/admissionregistration/v1"
	v13 "k8s.io/api/apps/v1"
	v14 "k8s.io/api/core/v1"
	v15 "k8s.io/api/rbac/v1"
	v16 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	v17 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	v18 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	v1beta1 "kubevirt.io/api/instancetype/v1beta1"
)

// Mock of APIServiceInterface interface
type MockAPIServiceInterface struct {
	ctrl     *gomock.Controller
	recorder *_MockAPIServiceInterfaceRecorder
}

// Recorder for MockAPIServiceInterface (not exported)
type _MockAPIServiceInterfaceRecorder struct {
	mock *MockAPIServiceInterface
}

func NewMockAPIServiceInterface(ctrl *gomock.Controller) *MockAPIServiceInterface {
	mock := &MockAPIServiceInterface{ctrl: ctrl}
	mock.recorder = &_MockAPIServiceInterfaceRecorder{mock}
	return mock
}

func (_m *MockAPIServiceInterface) EXPECT() *_MockAPIServiceInterfaceRecorder {
	return _m.recorder
}

func (_m *MockAPIServiceInterface) Get(ctx context.Context, name string, options v17.GetOptions) (*v18.APIService, error) {
	ret := _m.ctrl.Call(_m, "Get", ctx, name, options)
	ret0, _ := ret[0].(*v18.APIService)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAPIServiceInterfaceRecorder) Get(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Get", arg0, arg1, arg2)
}

func (_m *MockAPIServiceInterface) Create(ctx context.Context, apiService *v18.APIService, opts v17.CreateOptions) (*v18.APIService, error) {
	ret := _m.ctrl.Call(_m, "Create", ctx, apiService, opts)
	ret0, _ := ret[0].(*v18.APIService)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAPIServiceInterfaceRecorder) Create(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Create", arg0, arg1, arg2)
}

func (_m *MockAPIServiceInterface) Delete(ctx context.Context, name string, options v17.DeleteOptions) error {
	ret := _m.ctrl.Call(_m, "Delete", ctx, name, options)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockAPIServiceInterfaceRecorder) Delete(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Delete", arg0, arg1, arg2)
}

func (_m *MockAPIServiceInterface) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v17.PatchOptions, subresources ...string) (*v18.APIService, error) {
	_s := []interface{}{ctx, name, pt, data, opts}
	for _, _x := range subresources {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "Patch", _s...)
	ret0, _ := ret[0].(*v18.APIService)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAPIServiceInterfaceRecorder) Patch(arg0, arg1, arg2, arg3, arg4 interface{}, arg5 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1, arg2, arg3, arg4}, arg5...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Patch", _s...)
}

// Mock of StrategyInterface interface
type MockStrategyInterface struct {
	ctrl     *gomock.Controller
	recorder *_MockStrategyInterfaceRecorder
}

// Recorder for MockStrategyInterface (not exported)
type _MockStrategyInterfaceRecorder struct {
	mock *MockStrategyInterface
}

func NewMockStrategyInterface(ctrl *gomock.Controller) *MockStrategyInterface {
	mock := &MockStrategyInterface{ctrl: ctrl}
	mock.recorder = &_MockStrategyInterfaceRecorder{mock}
	return mock
}

func (_m *MockStrategyInterface) EXPECT() *_MockStrategyInterfaceRecorder {
	return _m.recorder
}

func (_m *MockStrategyInterface) ServiceAccounts() []*v14.ServiceAccount {
	ret := _m.ctrl.Call(_m, "ServiceAccounts")
	ret0, _ := ret[0].([]*v14.ServiceAccount)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ServiceAccounts() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ServiceAccounts")
}

func (_m *MockStrategyInterface) ClusterRoles() []*v15.ClusterRole {
	ret := _m.ctrl.Call(_m, "ClusterRoles")
	ret0, _ := ret[0].([]*v15.ClusterRole)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ClusterRoles() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ClusterRoles")
}

func (_m *MockStrategyInterface) ClusterRoleBindings() []*v15.ClusterRoleBinding {
	ret := _m.ctrl.Call(_m, "ClusterRoleBindings")
	ret0, _ := ret[0].([]*v15.ClusterRoleBinding)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ClusterRoleBindings() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ClusterRoleBindings")
}

func (_m *MockStrategyInterface) Roles() []*v15.Role {
	ret := _m.ctrl.Call(_m, "Roles")
	ret0, _ := ret[0].([]*v15.Role)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Roles() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Roles")
}

func (_m *MockStrategyInterface) RoleBindings() []*v15.RoleBinding {
	ret := _m.ctrl.Call(_m, "RoleBindings")
	ret0, _ := ret[0].([]*v15.RoleBinding)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) RoleBindings() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "RoleBindings")
}

func (_m *MockStrategyInterface) Services() []*v14.Service {
	ret := _m.ctrl.Call(_m, "Services")
	ret0, _ := ret[0].([]*v14.Service)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Services() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Services")
}

func (_m *MockStrategyInterface) Deployments() []*v13.Deployment {
	ret := _m.ctrl.Call(_m, "Deployments")
	ret0, _ := ret[0].([]*v13.Deployment)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Deployments() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Deployments")
}

func (_m *MockStrategyInterface) ApiDeployments() []*v13.Deployment {
	ret := _m.ctrl.Call(_m, "ApiDeployments")
	ret0, _ := ret[0].([]*v13.Deployment)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ApiDeployments() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ApiDeployments")
}

func (_m *MockStrategyInterface) ControllerDeployments() []*v13.Deployment {
	ret := _m.ctrl.Call(_m, "ControllerDeployments")
	ret0, _ := ret[0].([]*v13.Deployment)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ControllerDeployments() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ControllerDeployments")
}

func (_m *MockStrategyInterface) ExportProxyDeployments() []*v13.Deployment {
	ret := _m.ctrl.Call(_m, "ExportProxyDeployments")
	ret0, _ := ret[0].([]*v13.Deployment)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ExportProxyDeployments() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ExportProxyDeployments")
}

func (_m *MockStrategyInterface) DaemonSets() []*v13.DaemonSet {
	ret := _m.ctrl.Call(_m, "DaemonSets")
	ret0, _ := ret[0].([]*v13.DaemonSet)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) DaemonSets() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DaemonSets")
}

func (_m *MockStrategyInterface) ValidatingWebhookConfigurations() []*v12.ValidatingWebhookConfiguration {
	ret := _m.ctrl.Call(_m, "ValidatingWebhookConfigurations")
	ret0, _ := ret[0].([]*v12.ValidatingWebhookConfiguration)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ValidatingWebhookConfigurations() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ValidatingWebhookConfigurations")
}

func (_m *MockStrategyInterface) MutatingWebhookConfigurations() []*v12.MutatingWebhookConfiguration {
	ret := _m.ctrl.Call(_m, "MutatingWebhookConfigurations")
	ret0, _ := ret[0].([]*v12.MutatingWebhookConfiguration)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) MutatingWebhookConfigurations() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MutatingWebhookConfigurations")
}

func (_m *MockStrategyInterface) APIServices() []*v18.APIService {
	ret := _m.ctrl.Call(_m, "APIServices")
	ret0, _ := ret[0].([]*v18.APIService)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) APIServices() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "APIServices")
}

func (_m *MockStrategyInterface) CertificateSecrets() []*v14.Secret {
	ret := _m.ctrl.Call(_m, "CertificateSecrets")
	ret0, _ := ret[0].([]*v14.Secret)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) CertificateSecrets() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CertificateSecrets")
}

func (_m *MockStrategyInterface) SCCs() []*v10.SecurityContextConstraints {
	ret := _m.ctrl.Call(_m, "SCCs")
	ret0, _ := ret[0].([]*v10.SecurityContextConstraints)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) SCCs() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SCCs")
}

func (_m *MockStrategyInterface) ServiceMonitors() []*v11.ServiceMonitor {
	ret := _m.ctrl.Call(_m, "ServiceMonitors")
	ret0, _ := ret[0].([]*v11.ServiceMonitor)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ServiceMonitors() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ServiceMonitors")
}

func (_m *MockStrategyInterface) PrometheusRules() []*v11.PrometheusRule {
	ret := _m.ctrl.Call(_m, "PrometheusRules")
	ret0, _ := ret[0].([]*v11.PrometheusRule)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) PrometheusRules() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PrometheusRules")
}

func (_m *MockStrategyInterface) ConfigMaps() []*v14.ConfigMap {
	ret := _m.ctrl.Call(_m, "ConfigMaps")
	ret0, _ := ret[0].([]*v14.ConfigMap)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) ConfigMaps() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ConfigMaps")
}

func (_m *MockStrategyInterface) CRDs() []*v16.CustomResourceDefinition {
	ret := _m.ctrl.Call(_m, "CRDs")
	ret0, _ := ret[0].([]*v16.CustomResourceDefinition)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) CRDs() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CRDs")
}

func (_m *MockStrategyInterface) Routes() []*v1.Route {
	ret := _m.ctrl.Call(_m, "Routes")
	ret0, _ := ret[0].([]*v1.Route)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Routes() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Routes")
}

func (_m *MockStrategyInterface) Instancetypes() []*v1beta1.VirtualMachineClusterInstancetype {
	ret := _m.ctrl.Call(_m, "Instancetypes")
	ret0, _ := ret[0].([]*v1beta1.VirtualMachineClusterInstancetype)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Instancetypes() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Instancetypes")
}

func (_m *MockStrategyInterface) Preferences() []*v1beta1.VirtualMachineClusterPreference {
	ret := _m.ctrl.Call(_m, "Preferences")
	ret0, _ := ret[0].([]*v1beta1.VirtualMachineClusterPreference)
	return ret0
}

func (_mr *_MockStrategyInterfaceRecorder) Preferences() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Preferences")
}
