import { AuthShell } from "@/components/auth-shell";
import { UpdatePasswordForm } from "@/components/auth-forms";

export default function UpdatePasswordPage() {
  return <AuthShell eyebrow="Account recovery" title="Choose a new password." description="This page only works after you open a valid recovery link from Supabase Auth."><UpdatePasswordForm /></AuthShell>;
}
