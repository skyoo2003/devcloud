// SPDX-License-Identifier: Apache-2.0

// internal/services/transcribe/provider.go
package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the Transcribe service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "transcribe" }
func (p *Provider) ServiceName() string           { return "Transcribe" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "transcribe"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// TranscriptionJob
	case "StartTranscriptionJob":
		return p.startTranscriptionJob(params)
	case "GetTranscriptionJob":
		return p.getTranscriptionJob(params)
	case "ListTranscriptionJobs":
		return p.listTranscriptionJobs(params)
	case "DeleteTranscriptionJob":
		return p.deleteTranscriptionJob(params)
	// Vocabulary
	case "CreateVocabulary":
		return p.createVocabulary(params)
	case "GetVocabulary":
		return p.getVocabulary(params)
	case "ListVocabularies":
		return p.listVocabularies(params)
	case "UpdateVocabulary":
		return p.updateVocabulary(params)
	case "DeleteVocabulary":
		return p.deleteVocabulary(params)
	// VocabularyFilter
	case "CreateVocabularyFilter":
		return p.createVocabularyFilter(params)
	case "GetVocabularyFilter":
		return p.getVocabularyFilter(params)
	case "ListVocabularyFilters":
		return p.listVocabularyFilters(params)
	case "UpdateVocabularyFilter":
		return p.updateVocabularyFilter(params)
	case "DeleteVocabularyFilter":
		return p.deleteVocabularyFilter(params)
	// LanguageModel
	case "CreateLanguageModel":
		return p.createLanguageModel(params)
	case "DescribeLanguageModel":
		return p.describeLanguageModel(params)
	case "ListLanguageModels":
		return p.listLanguageModels(params)
	case "DeleteLanguageModel":
		return p.deleteLanguageModel(params)
	// CallAnalyticsCategory
	case "CreateCallAnalyticsCategory":
		return p.createCallAnalyticsCategory(params)
	case "GetCallAnalyticsCategory":
		return p.getCallAnalyticsCategory(params)
	case "ListCallAnalyticsCategories":
		return p.listCallAnalyticsCategories(params)
	case "UpdateCallAnalyticsCategory":
		return p.updateCallAnalyticsCategory(params)
	case "DeleteCallAnalyticsCategory":
		return p.deleteCallAnalyticsCategory(params)
	// CallAnalyticsJob (reuse transcription_jobs table with same logic)
	case "StartCallAnalyticsJob":
		return p.startCallAnalyticsJob(params)
	case "GetCallAnalyticsJob":
		return p.getCallAnalyticsJob(params)
	case "ListCallAnalyticsJobs":
		return p.listCallAnalyticsJobs(params)
	case "DeleteCallAnalyticsJob":
		return p.deleteCallAnalyticsJob(params)
	// MedicalTranscriptionJob (reuse transcription_jobs logic)
	case "StartMedicalTranscriptionJob":
		return p.startMedicalTranscriptionJob(params)
	case "GetMedicalTranscriptionJob":
		return p.getMedicalTranscriptionJob(params)
	case "ListMedicalTranscriptionJobs":
		return p.listMedicalTranscriptionJobs(params)
	case "DeleteMedicalTranscriptionJob":
		return p.deleteMedicalTranscriptionJob(params)
	// MedicalVocabulary (reuse vocabularies logic)
	case "CreateMedicalVocabulary":
		return p.createMedicalVocabulary(params)
	case "GetMedicalVocabulary":
		return p.getMedicalVocabulary(params)
	case "ListMedicalVocabularies":
		return p.listMedicalVocabularies(params)
	case "UpdateMedicalVocabulary":
		return p.updateMedicalVocabulary(params)
	case "DeleteMedicalVocabulary":
		return p.deleteMedicalVocabulary(params)
	// MedicalScribeJob (reuse transcription_jobs logic)
	case "StartMedicalScribeJob":
		return p.startMedicalScribeJob(params)
	case "GetMedicalScribeJob":
		return p.getMedicalScribeJob(params)
	case "ListMedicalScribeJobs":
		return p.listMedicalScribeJobs(params)
	case "DeleteMedicalScribeJob":
		return p.deleteMedicalScribeJob(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	jobs, err := p.store.ListTranscriptionJobs("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(jobs))
	for _, j := range jobs {
		res = append(res, plugin.Resource{Type: "transcription-job", ID: j.Name, Name: j.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- TranscriptionJob ----

func (p *Provider) startTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "TranscriptionJobName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	mediaURI := ""
	mediaFormat := "mp4"
	if media, ok := params["Media"].(map[string]any); ok {
		mediaURI, _ = media["MediaFileUri"].(string)
	}
	if mf, ok := params["MediaFormat"].(string); ok && mf != "" {
		mediaFormat = mf
	}
	j, err := p.store.CreateTranscriptionJob(name, language, mediaURI, mediaFormat)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "transcription job already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("transcribe", "transcription-job", name)
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseListTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TranscriptionJob": transcriptionJobToMap(j),
	})
}

func (p *Provider) getTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "TranscriptionJobName is required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetTranscriptionJob(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "transcription job not found", http.StatusBadRequest), nil
	}
	m := transcriptionJobToMap(j)
	// Dummy transcript result
	m["Transcript"] = map[string]any{
		"TranscriptFileUri": "s3://devcloud-transcribe/" + name + "/transcript.json",
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TranscriptionJob": m})
}

func (p *Provider) listTranscriptionJobs(params map[string]any) (*plugin.Response, error) {
	statusFilter, _ := params["Status"].(string)
	jobs, err := p.store.ListTranscriptionJobs(statusFilter)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, transcriptionJobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TranscriptionJobSummaries": items,
	})
}

func (p *Provider) deleteTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "TranscriptionJobName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("transcribe", "transcription-job", name)
	p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteTranscriptionJob(name); err != nil {
		return shared.JSONError("NotFoundException", "transcription job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Vocabulary ----

func (p *Provider) createVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	phrases := "[]"
	if p2, ok := params["Phrases"]; ok {
		b, _ := json.Marshal(p2)
		phrases = string(b)
	}
	v, err := p.store.CreateVocabulary(name, language, phrases)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "vocabulary already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, vocabularyToMap(v))
}

func (p *Provider) getVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetVocabulary(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "vocabulary not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, vocabularyToMap(v))
}

func (p *Provider) listVocabularies(params map[string]any) (*plugin.Response, error) {
	language, _ := params["LanguageCode"].(string)
	vocabs, err := p.store.ListVocabularies(language)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(vocabs))
	for _, v := range vocabs {
		items = append(items, vocabularyToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Vocabularies": items})
}

func (p *Provider) updateVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetVocabulary(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "vocabulary not found", http.StatusBadRequest), nil
	}
	language := v.Language
	if l, ok := params["LanguageCode"].(string); ok && l != "" {
		language = l
	}
	phrases := v.Phrases
	if p2, ok := params["Phrases"]; ok {
		b, _ := json.Marshal(p2)
		phrases = string(b)
	}
	if err := p.store.UpdateVocabulary(name, language, phrases); err != nil {
		return shared.JSONError("NotFoundException", "vocabulary not found", http.StatusBadRequest), nil
	}
	updated, _ := p.store.GetVocabulary(name)
	return shared.JSONResponse(http.StatusOK, vocabularyToMap(updated))
}

func (p *Provider) deleteVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVocabulary(name); err != nil {
		return shared.JSONError("NotFoundException", "vocabulary not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- VocabularyFilter ----

func (p *Provider) createVocabularyFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyFilterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyFilterName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	words := "[]"
	if w, ok := params["Words"]; ok {
		b, _ := json.Marshal(w)
		words = string(b)
	}
	f, err := p.store.CreateVocabularyFilter(name, language, words)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "vocabulary filter already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, vocabularyFilterToMap(f))
}

func (p *Provider) getVocabularyFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyFilterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyFilterName is required", http.StatusBadRequest), nil
	}
	f, err := p.store.GetVocabularyFilter(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "vocabulary filter not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, vocabularyFilterToMap(f))
}

func (p *Provider) listVocabularyFilters(params map[string]any) (*plugin.Response, error) {
	language, _ := params["LanguageCode"].(string)
	filters, err := p.store.ListVocabularyFilters(language)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		items = append(items, vocabularyFilterToMap(&f))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"VocabularyFilters": items})
}

func (p *Provider) updateVocabularyFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyFilterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyFilterName is required", http.StatusBadRequest), nil
	}
	f, err := p.store.GetVocabularyFilter(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "vocabulary filter not found", http.StatusBadRequest), nil
	}
	language := f.Language
	if l, ok := params["LanguageCode"].(string); ok && l != "" {
		language = l
	}
	words := f.Words
	if w, ok := params["Words"]; ok {
		b, _ := json.Marshal(w)
		words = string(b)
	}
	if err := p.store.UpdateVocabularyFilter(name, language, words); err != nil {
		return shared.JSONError("NotFoundException", "vocabulary filter not found", http.StatusBadRequest), nil
	}
	updated, _ := p.store.GetVocabularyFilter(name)
	return shared.JSONResponse(http.StatusOK, vocabularyFilterToMap(updated))
}

func (p *Provider) deleteVocabularyFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyFilterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyFilterName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVocabularyFilter(name); err != nil {
		return shared.JSONError("NotFoundException", "vocabulary filter not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- LanguageModel ----

func (p *Provider) createLanguageModel(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ModelName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ModelName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	baseModel, _ := params["BaseModelName"].(string)
	if baseModel == "" {
		baseModel = "NarrowBand"
	}
	m, err := p.store.CreateLanguageModel(name, language, baseModel)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "language model already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ModelName":     m.Name,
		"LanguageCode":  m.Language,
		"BaseModelName": m.BaseModel,
		"ModelStatus":   m.Status,
	})
}

func (p *Provider) describeLanguageModel(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ModelName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ModelName is required", http.StatusBadRequest), nil
	}
	m, err := p.store.GetLanguageModel(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "language model not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LanguageModel": languageModelToMap(m),
	})
}

func (p *Provider) listLanguageModels(params map[string]any) (*plugin.Response, error) {
	language, _ := params["LanguageCode"].(string)
	models, err := p.store.ListLanguageModels(language)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(models))
	for _, m := range models {
		items = append(items, languageModelToMap(&m))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Models": items})
}

func (p *Provider) deleteLanguageModel(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ModelName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ModelName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLanguageModel(name); err != nil {
		return shared.JSONError("NotFoundException", "language model not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- CallAnalyticsCategory ----

func (p *Provider) createCallAnalyticsCategory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CategoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CategoryName is required", http.StatusBadRequest), nil
	}
	inputType, _ := params["InputType"].(string)
	if inputType == "" {
		inputType = "REAL_TIME"
	}
	rules := "[]"
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		rules = string(b)
	}
	cat, err := p.store.CreateCallAnalyticsCategory(name, rules, inputType)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "category already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CategoryProperties": callAnalyticsCategoryToMap(cat),
	})
}

func (p *Provider) getCallAnalyticsCategory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CategoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CategoryName is required", http.StatusBadRequest), nil
	}
	cat, err := p.store.GetCallAnalyticsCategory(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "category not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CategoryProperties": callAnalyticsCategoryToMap(cat),
	})
}

func (p *Provider) listCallAnalyticsCategories(_ map[string]any) (*plugin.Response, error) {
	cats, err := p.store.ListCallAnalyticsCategories()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(cats))
	for _, c := range cats {
		items = append(items, callAnalyticsCategoryToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Categories": items})
}

func (p *Provider) updateCallAnalyticsCategory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CategoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CategoryName is required", http.StatusBadRequest), nil
	}
	cat, err := p.store.GetCallAnalyticsCategory(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "category not found", http.StatusBadRequest), nil
	}
	inputType := cat.InputType
	if it, ok := params["InputType"].(string); ok && it != "" {
		inputType = it
	}
	rules := cat.Rules
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		rules = string(b)
	}
	if err := p.store.UpdateCallAnalyticsCategory(name, rules, inputType); err != nil {
		return shared.JSONError("NotFoundException", "category not found", http.StatusBadRequest), nil
	}
	updated, _ := p.store.GetCallAnalyticsCategory(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CategoryProperties": callAnalyticsCategoryToMap(updated),
	})
}

func (p *Provider) deleteCallAnalyticsCategory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CategoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CategoryName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCallAnalyticsCategory(name); err != nil {
		return shared.JSONError("NotFoundException", "category not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- CallAnalyticsJob (delegates to transcription_jobs table with "call-analytics-job" prefix) ----

func (p *Provider) startCallAnalyticsJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CallAnalyticsJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CallAnalyticsJobName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	mediaURI := ""
	if media, ok := params["Media"].(map[string]any); ok {
		mediaURI, _ = media["MediaFileUri"].(string)
	}
	key := "call-analytics:" + name
	j, err := p.store.CreateTranscriptionJob(key, language, mediaURI, "mp4")
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "call analytics job already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CallAnalyticsJob": callAnalyticsJobToMap(name, j),
	})
}

func (p *Provider) getCallAnalyticsJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CallAnalyticsJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CallAnalyticsJobName is required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetTranscriptionJob("call-analytics:" + name)
	if err != nil {
		return shared.JSONError("NotFoundException", "call analytics job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CallAnalyticsJob": callAnalyticsJobToMap(name, j),
	})
}

func (p *Provider) listCallAnalyticsJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListTranscriptionJobs("")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for _, j := range jobs {
		if strings.HasPrefix(j.Name, "call-analytics:") {
			displayName := strings.TrimPrefix(j.Name, "call-analytics:")
			items = append(items, callAnalyticsJobToMap(displayName, &j))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"CallAnalyticsJobSummaries": items})
}

func (p *Provider) deleteCallAnalyticsJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["CallAnalyticsJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "CallAnalyticsJobName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTranscriptionJob("call-analytics:" + name); err != nil {
		return shared.JSONError("NotFoundException", "call analytics job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- MedicalTranscriptionJob ----

func (p *Provider) startMedicalTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalTranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalTranscriptionJobName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	mediaURI := ""
	if media, ok := params["Media"].(map[string]any); ok {
		mediaURI, _ = media["MediaFileUri"].(string)
	}
	key := "medical:" + name
	j, err := p.store.CreateTranscriptionJob(key, language, mediaURI, "mp4")
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "medical transcription job already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MedicalTranscriptionJob": medicalTranscriptionJobToMap(name, j),
	})
}

func (p *Provider) getMedicalTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalTranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalTranscriptionJobName is required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetTranscriptionJob("medical:" + name)
	if err != nil {
		return shared.JSONError("NotFoundException", "medical transcription job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MedicalTranscriptionJob": medicalTranscriptionJobToMap(name, j),
	})
}

func (p *Provider) listMedicalTranscriptionJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListTranscriptionJobs("")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for _, j := range jobs {
		if strings.HasPrefix(j.Name, "medical:") {
			displayName := strings.TrimPrefix(j.Name, "medical:")
			items = append(items, medicalTranscriptionJobToMap(displayName, &j))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MedicalTranscriptionJobSummaries": items})
}

func (p *Provider) deleteMedicalTranscriptionJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalTranscriptionJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalTranscriptionJobName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTranscriptionJob("medical:" + name); err != nil {
		return shared.JSONError("NotFoundException", "medical transcription job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- MedicalVocabulary ----

func (p *Provider) createMedicalVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	language, _ := params["LanguageCode"].(string)
	if language == "" {
		language = "en-US"
	}
	key := "medical:" + name
	v, err := p.store.CreateVocabulary(key, language, "[]")
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "medical vocabulary already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, medicalVocabularyToMap(name, v))
}

func (p *Provider) getMedicalVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetVocabulary("medical:" + name)
	if err != nil {
		return shared.JSONError("NotFoundException", "medical vocabulary not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, medicalVocabularyToMap(name, v))
}

func (p *Provider) listMedicalVocabularies(_ map[string]any) (*plugin.Response, error) {
	vocabs, err := p.store.ListVocabularies("")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for _, v := range vocabs {
		if strings.HasPrefix(v.Name, "medical:") {
			displayName := strings.TrimPrefix(v.Name, "medical:")
			items = append(items, medicalVocabularyToMap(displayName, &v))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Vocabularies": items})
}

func (p *Provider) updateMedicalVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	key := "medical:" + name
	v, err := p.store.GetVocabulary(key)
	if err != nil {
		return shared.JSONError("NotFoundException", "medical vocabulary not found", http.StatusBadRequest), nil
	}
	language := v.Language
	if l, ok := params["LanguageCode"].(string); ok && l != "" {
		language = l
	}
	if err := p.store.UpdateVocabulary(key, language, "[]"); err != nil {
		return shared.JSONError("NotFoundException", "medical vocabulary not found", http.StatusBadRequest), nil
	}
	updated, _ := p.store.GetVocabulary(key)
	return shared.JSONResponse(http.StatusOK, medicalVocabularyToMap(name, updated))
}

func (p *Provider) deleteMedicalVocabulary(params map[string]any) (*plugin.Response, error) {
	name, _ := params["VocabularyName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "VocabularyName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVocabulary("medical:" + name); err != nil {
		return shared.JSONError("NotFoundException", "medical vocabulary not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- MedicalScribeJob ----

func (p *Provider) startMedicalScribeJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalScribeJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalScribeJobName is required", http.StatusBadRequest), nil
	}
	mediaURI := ""
	if media, ok := params["Media"].(map[string]any); ok {
		mediaURI, _ = media["MediaFileUri"].(string)
	}
	key := "medical-scribe:" + name
	j, err := p.store.CreateTranscriptionJob(key, "en-US", mediaURI, "mp4")
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ConflictException", "medical scribe job already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MedicalScribeJob": medicalScribeJobToMap(name, j),
	})
}

func (p *Provider) getMedicalScribeJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalScribeJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalScribeJobName is required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetTranscriptionJob("medical-scribe:" + name)
	if err != nil {
		return shared.JSONError("NotFoundException", "medical scribe job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MedicalScribeJob": medicalScribeJobToMap(name, j),
	})
}

func (p *Provider) listMedicalScribeJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListTranscriptionJobs("")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for _, j := range jobs {
		if strings.HasPrefix(j.Name, "medical-scribe:") {
			displayName := strings.TrimPrefix(j.Name, "medical-scribe:")
			items = append(items, medicalScribeJobToMap(displayName, &j))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MedicalScribeJobSummaries": items})
}

func (p *Provider) deleteMedicalScribeJob(params map[string]any) (*plugin.Response, error) {
	name, _ := params["MedicalScribeJobName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MedicalScribeJobName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTranscriptionJob("medical-scribe:" + name); err != nil {
		return shared.JSONError("NotFoundException", "medical scribe job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Tags ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseListTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// ---- helpers ----

func transcriptionJobToMap(j *TranscriptionJob) map[string]any {
	return map[string]any{
		"TranscriptionJobName":   j.Name,
		"TranscriptionJobStatus": j.Status,
		"LanguageCode":           j.Language,
		"MediaFormat":            j.MediaFormat,
		"Media":                  map[string]any{"MediaFileUri": j.MediaURI},
		"CreationTime":           j.CreatedAt.Unix(),
		"CompletionTime":         j.CompletedAt.Unix(),
	}
}

func vocabularyToMap(v *Vocabulary) map[string]any {
	return map[string]any{
		"VocabularyName":   v.Name,
		"LanguageCode":     v.Language,
		"VocabularyState":  v.Status,
		"LastModifiedTime": v.UpdatedAt.Unix(),
	}
}

func vocabularyFilterToMap(f *VocabularyFilter) map[string]any {
	return map[string]any{
		"VocabularyFilterName": f.Name,
		"LanguageCode":         f.Language,
		"LastModifiedTime":     f.UpdatedAt.Unix(),
	}
}

func languageModelToMap(m *LanguageModel) map[string]any {
	return map[string]any{
		"ModelName":     m.Name,
		"LanguageCode":  m.Language,
		"BaseModelName": m.BaseModel,
		"ModelStatus":   m.Status,
		"CreateTime":    m.CreatedAt.Unix(),
	}
}

func callAnalyticsCategoryToMap(c *CallAnalyticsCategory) map[string]any {
	return map[string]any{
		"CategoryName":   c.Name,
		"Rules":          jsonParse(c.Rules),
		"InputType":      c.InputType,
		"CreateTime":     c.CreatedAt.Unix(),
		"LastUpdateTime": c.UpdatedAt.Unix(),
	}
}

func callAnalyticsJobToMap(displayName string, j *TranscriptionJob) map[string]any {
	return map[string]any{
		"CallAnalyticsJobName":   displayName,
		"CallAnalyticsJobStatus": j.Status,
		"LanguageCode":           j.Language,
		"Media":                  map[string]any{"MediaFileUri": j.MediaURI},
		"CreationTime":           j.CreatedAt.Unix(),
		"CompletionTime":         j.CompletedAt.Unix(),
	}
}

func medicalTranscriptionJobToMap(displayName string, j *TranscriptionJob) map[string]any {
	return map[string]any{
		"MedicalTranscriptionJobName": displayName,
		"TranscriptionJobStatus":      j.Status,
		"LanguageCode":                j.Language,
		"Media":                       map[string]any{"MediaFileUri": j.MediaURI},
		"CreationTime":                j.CreatedAt.Unix(),
		"CompletionTime":              j.CompletedAt.Unix(),
	}
}

func medicalVocabularyToMap(displayName string, v *Vocabulary) map[string]any {
	return map[string]any{
		"VocabularyName":   displayName,
		"LanguageCode":     v.Language,
		"VocabularyState":  v.Status,
		"LastModifiedTime": v.UpdatedAt.Unix(),
	}
}

func medicalScribeJobToMap(displayName string, j *TranscriptionJob) map[string]any {
	return map[string]any{
		"MedicalScribeJobName":   displayName,
		"MedicalScribeJobStatus": j.Status,
		"Media":                  map[string]any{"MediaFileUri": j.MediaURI},
		"CreationTime":           j.CreatedAt.Unix(),
		"CompletionTime":         j.CompletedAt.Unix(),
	}
}

func jsonParse(s string) any {
	var v any
	json.Unmarshal([]byte(s), &v)
	return v
}

func parseListTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
